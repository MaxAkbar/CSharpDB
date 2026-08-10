using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class QueryResultLifecycleTests
{
    private static CancellationToken TestCancellationToken => TestContext.Current.CancellationToken;

    private static readonly ColumnDefinition[] SingleColumnSchema =
    [
        new ColumnDefinition { Name = "value", Type = DbType.Integer },
    ];

    [Fact]
    public async Task MoveNextAsync_ReportsRowsAndExhaustionExactlyOnce()
    {
        var observer = new RecordingObserver();
        var result = new QueryResult(new SequenceOperator(1, 2));
        result.SetObserver(observer);

        Assert.True(await result.MoveNextAsync(TestCancellationToken));
        Assert.True(await result.MoveNextAsync(TestCancellationToken));
        Assert.False(await result.MoveNextAsync(TestCancellationToken));
        Assert.False(await result.MoveNextAsync(TestCancellationToken));
        await result.DisposeAsync();
        await result.DisposeAsync();

        Assert.Equal(1, observer.FirstRowCount);
        Assert.Equal(2, observer.RowCount);
        AssertCompletion(observer, QueryResultCompletionReason.Exhausted, 2);
    }

    [Fact]
    public async Task ToListAsync_SyncLookupAndScalar_ReportRowsAndExhaustion()
    {
        var lookupObserver = new RecordingObserver();
        QueryResult lookup = QueryResult.FromSyncLookup(
            [DbValue.FromInteger(42)],
            SingleColumnSchema);
        lookup.SetObserver(lookupObserver);

        List<DbValue[]> lookupRows = await lookup.ToListAsync(TestCancellationToken);

        Assert.Single(lookupRows);
        Assert.Equal(1, lookupObserver.FirstRowCount);
        Assert.Equal(1, lookupObserver.RowCount);
        AssertCompletion(lookupObserver, QueryResultCompletionReason.Exhausted, 1);

        var scalarObserver = new RecordingObserver();
        QueryResult scalar = QueryResult.FromSyncScalar(
            DbValue.FromInteger(7),
            SingleColumnSchema);
        scalar.SetObserver(scalarObserver);

        List<DbValue[]> scalarRows = await scalar.ToListAsync(TestCancellationToken);

        Assert.Single(scalarRows);
        Assert.Equal(1, scalarObserver.FirstRowCount);
        Assert.Equal(1, scalarObserver.RowCount);
        AssertCompletion(scalarObserver, QueryResultCompletionReason.Exhausted, 1);
    }

    [Fact]
    public async Task ToListAsync_EmptySyncLookup_ReportsZeroRowExhaustion()
    {
        var observer = new RecordingObserver();
        QueryResult result = QueryResult.FromSyncLookup(null, SingleColumnSchema);
        result.SetObserver(observer);

        List<DbValue[]> rows = await result.ToListAsync(TestCancellationToken);

        Assert.Empty(rows);
        Assert.Equal(0, observer.FirstRowCount);
        Assert.Equal(0, observer.RowCount);
        AssertCompletion(observer, QueryResultCompletionReason.Exhausted, 0);
    }

    [Fact]
    public async Task ToListAsync_MaterializedRows_ReportsEveryRowAndExhaustion()
    {
        var observer = new RecordingObserver();
        QueryResult result = QueryResult.FromMaterializedRows(
            SingleColumnSchema,
            CreateRows(1, 2, 3));
        result.SetObserver(observer);

        List<DbValue[]> rows = await result.ToListAsync(TestCancellationToken);

        Assert.Equal(3, rows.Count);
        Assert.Equal(1, observer.FirstRowCount);
        Assert.Equal(3, observer.RowCount);
        AssertCompletion(observer, QueryResultCompletionReason.Exhausted, 3);
    }

    [Fact]
    public async Task ToListAsync_StreamingOperator_ReportsEveryRowAndExhaustion()
    {
        var observer = new RecordingObserver();
        var result = new QueryResult(new SequenceOperator(1, 2, 3));
        result.SetObserver(observer);

        List<DbValue[]> rows = await result.ToListAsync(TestCancellationToken);

        Assert.Equal(3, rows.Count);
        Assert.Equal(1, observer.FirstRowCount);
        Assert.Equal(3, observer.RowCount);
        AssertCompletion(observer, QueryResultCompletionReason.Exhausted, 3);
    }

    [Fact]
    public async Task ToListAsync_BatchRows_ReportsEveryRowAndExhaustion()
    {
        var observer = new RecordingObserver();
        QueryResult result = QueryResult.FromBatchOperator(
            new SequenceBatchOperator(CreateBatch(1, 2), CreateBatch(3, 4)));
        result.SetObserver(observer);

        List<DbValue[]> rows = await result.ToListAsync(TestCancellationToken);

        Assert.Equal(4, rows.Count);
        Assert.Equal(1, observer.FirstRowCount);
        Assert.Equal(4, observer.RowCount);
        AssertCompletion(observer, QueryResultCompletionReason.Exhausted, 4);
    }

    [Fact]
    public async Task MoveNextAsync_BatchRows_ReportsEveryRowAndExhaustion()
    {
        var observer = new RecordingObserver();
        QueryResult result = QueryResult.FromBatchOperator(
            new SequenceBatchOperator(CreateBatch(1, 2), CreateBatch(3)));
        result.SetObserver(observer);

        Assert.True(await result.MoveNextAsync(TestCancellationToken));
        Assert.True(await result.MoveNextAsync(TestCancellationToken));
        Assert.True(await result.MoveNextAsync(TestCancellationToken));
        Assert.False(await result.MoveNextAsync(TestCancellationToken));

        Assert.Equal(1, observer.FirstRowCount);
        Assert.Equal(3, observer.RowCount);
        AssertCompletion(observer, QueryResultCompletionReason.Exhausted, 3);
    }

    [Fact]
    public async Task ToListAsync_BatchBackedOperator_ReportsEveryRowAndExhaustion()
    {
        var observer = new RecordingObserver();
        var batchSource = new SequenceBatchOperator(CreateBatch(1), CreateBatch(2, 3));
        var result = new QueryResult(new BatchBackedOperator(batchSource));
        result.SetObserver(observer);

        List<DbValue[]> rows = await result.ToListAsync(TestCancellationToken);

        Assert.Equal(3, rows.Count);
        Assert.Equal(1, observer.FirstRowCount);
        Assert.Equal(3, observer.RowCount);
        AssertCompletion(observer, QueryResultCompletionReason.Exhausted, 3);
    }

    [Fact]
    public async Task ToListAsync_DirectMaterializedBatch_ReportsEveryRowAndExhaustion()
    {
        var observer = new RecordingObserver();
        QueryResult result = QueryResult.FromBatchOperator(
            new MaterializedBatchOperator(CreateRows(1, 2, 3)));
        result.SetObserver(observer);

        List<DbValue[]> rows = await result.ToListAsync(TestCancellationToken);

        Assert.Equal(3, rows.Count);
        Assert.Equal(1, observer.FirstRowCount);
        Assert.Equal(3, observer.RowCount);
        AssertCompletion(observer, QueryResultCompletionReason.Exhausted, 3);
    }

    [Fact]
    public async Task ToListAsync_WhenOperatorFails_ReportsPartialRowsAndFailure()
    {
        var observer = new RecordingObserver();
        var result = new QueryResult(new FaultingOperator());
        result.SetObserver(observer);

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => result.ToListAsync(TestCancellationToken).AsTask());
        await result.DisposeAsync();

        Assert.Equal(1, observer.FirstRowCount);
        Assert.Equal(1, observer.RowCount);
        QueryResultCompletion completion = AssertCompletion(
            observer,
            QueryResultCompletionReason.Failed,
            1);
        Assert.Same(error, completion.Error);
    }

    [Fact]
    public async Task ToListAsync_WhenBatchFails_ReportsPartialRowsAndFailure()
    {
        var observer = new RecordingObserver();
        QueryResult result = QueryResult.FromBatchOperator(
            new FaultingBatchOperator(CreateBatch(1, 2)));
        result.SetObserver(observer);

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => result.ToListAsync(TestCancellationToken).AsTask());

        Assert.Equal(1, observer.FirstRowCount);
        Assert.Equal(2, observer.RowCount);
        QueryResultCompletion completion = AssertCompletion(
            observer,
            QueryResultCompletionReason.Failed,
            2);
        Assert.Same(error, completion.Error);
    }

    [Fact]
    public async Task MoveNextAsync_WhenCanceled_ReportsCancellationExactlyOnce()
    {
        using var source = new CancellationTokenSource();
        source.Cancel();
        var observer = new RecordingObserver();
        var result = new QueryResult(new SequenceOperator(1));
        result.SetObserver(observer);

        OperationCanceledException error = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => result.MoveNextAsync(source.Token).AsTask());
        await result.DisposeAsync();

        QueryResultCompletion completion = AssertCompletion(
            observer,
            QueryResultCompletionReason.Canceled,
            0);
        Assert.Same(error, completion.Error);
    }

    [Fact]
    public async Task GetRowsAsync_WhenCurrentFails_ReportsProducedRowAndFailure()
    {
        var observer = new RecordingObserver();
        var result = new QueryResult(new CurrentFaultOperator());
        result.SetObserver(observer);
        await using IAsyncEnumerator<DbValue[]> enumerator =
            result.GetRowsAsync(TestCancellationToken).GetAsyncEnumerator(TestCancellationToken);

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => enumerator.MoveNextAsync().AsTask());
        await result.DisposeAsync();

        Assert.Equal(1, observer.FirstRowCount);
        Assert.Equal(1, observer.RowCount);
        QueryResultCompletion completion = AssertCompletion(
            observer,
            QueryResultCompletionReason.Failed,
            1);
        Assert.Same(error, completion.Error);
    }

    [Fact]
    public async Task DisposeAsync_EarlyAndNeverOpened_ReportDisposal()
    {
        var earlyObserver = new RecordingObserver();
        var earlyOperator = new SequenceOperator(1, 2);
        var earlyResult = new QueryResult(earlyOperator);
        earlyResult.SetObserver(earlyObserver);

        Assert.True(await earlyResult.MoveNextAsync(TestCancellationToken));
        await earlyResult.DisposeAsync();

        Assert.True(earlyOperator.Disposed);
        AssertCompletion(earlyObserver, QueryResultCompletionReason.Disposed, 1);

        var unopenedObserver = new RecordingObserver();
        var unopenedOperator = new SequenceOperator(1);
        var unopenedResult = new QueryResult(unopenedOperator);
        unopenedResult.SetObserver(unopenedObserver);

        await unopenedResult.DisposeAsync();

        Assert.False(unopenedOperator.Opened);
        Assert.True(unopenedOperator.Disposed);
        AssertCompletion(unopenedObserver, QueryResultCompletionReason.Disposed, 0);
    }

    [Fact]
    public async Task ThrowingObserver_DoesNotChangeEnumerationOrDisposalSemantics()
    {
        var observer = new ThrowingObserver();
        var result = new QueryResult(new SequenceOperator(1));
        int disposeCallbackCount = 0;
        result.SetObserver(observer);
        result.SetDisposeCallback(
            () =>
            {
                disposeCallbackCount++;
                return ValueTask.CompletedTask;
            });

        Assert.True(await result.MoveNextAsync(TestCancellationToken));
        Assert.False(await result.MoveNextAsync(TestCancellationToken));
        await result.DisposeAsync();

        Assert.Equal(1, observer.FirstRowAttempts);
        Assert.Equal(1, observer.RowAttempts);
        Assert.Equal(1, observer.CompletionAttempts);
        Assert.Equal(1, disposeCallbackCount);
    }

    [Fact]
    public void SetObserver_RejectsDmlResultsAndDuplicateOrLateRegistration()
    {
        var observer = new RecordingObserver();
        QueryResult zero = QueryResult.FromRowsAffected(0);
        QueryResult one = QueryResult.FromRowsAffected(1);

        Assert.Same(zero, QueryResult.FromRowsAffected(0));
        Assert.Same(one, QueryResult.FromRowsAffected(1));
        Assert.Throws<InvalidOperationException>(() => zero.SetObserver(observer));
        Assert.Throws<InvalidOperationException>(() => one.SetObserver(observer));
        Assert.Throws<InvalidOperationException>(() => new QueryResult(2).SetObserver(observer));

        var query = new QueryResult(new SequenceOperator(1));
        query.SetObserver(observer);
        Assert.Throws<InvalidOperationException>(
            () => query.SetObserver(new RecordingObserver()));
        var lateQuery = new QueryResult(new SequenceOperator(1));
        _ = lateQuery.MoveNextAsync(TestCancellationToken);
        Assert.Throws<InvalidOperationException>(
            () => lateQuery.SetObserver(new RecordingObserver()));
    }

    [Fact]
    public async Task SetObserver_RejectsRegistrationAfterUnobservedOpenFailure()
    {
        var result = new QueryResult(new ThrowingOpenOperator());

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => result.MoveNextAsync(TestCancellationToken).AsTask());

        Assert.Throws<InvalidOperationException>(
            () => result.SetObserver(new RecordingObserver()));
    }

    [Fact]
    public async Task ConcurrentExhaustionAndDisposal_InvokeOneTerminalCallback()
    {
        var observer = new RecordingObserver();
        var op = new BlockingExhaustionOperator();
        var result = new QueryResult(op);
        result.SetObserver(observer);

        Assert.True(await result.MoveNextAsync(TestCancellationToken));
        Task<bool> exhaustion = result.MoveNextAsync(TestCancellationToken).AsTask();
        await op.ExhaustionEntered;
        Task disposal = result.DisposeAsync().AsTask();
        op.ReleaseExhaustion();

        Assert.False(await exhaustion);
        await disposal;

        QueryResultCompletion completion = Assert.Single(observer.Completions);
        Assert.Contains(
            completion.Reason,
            new[] { QueryResultCompletionReason.Exhausted, QueryResultCompletionReason.Disposed });
        Assert.Equal(1, completion.RowsProduced);
    }

    [Fact]
    public async Task TerminalObserver_CanSynchronouslyWaitForReentrantDisposal()
    {
        var observer = new ReentrantDisposalObserver();
        var result = new QueryResult(new SequenceOperator(1));
        observer.Result = result;
        result.SetObserver(observer);

        List<DbValue[]> rows = await result.ToListAsync(TestCancellationToken)
            .AsTask()
            .WaitAsync(TimeSpan.FromSeconds(5), TestCancellationToken);

        Assert.Single(rows);
        Assert.True(observer.DisposalCompletedInsideCallback);
        await observer.DisposalTask.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestCancellationToken);
    }

    [Fact]
    public async Task DisposeAsync_WhenOperatorDisposeThrows_StillInvokesCallback()
    {
        var observer = new ThrowingObserver();
        var result = new QueryResult(new ThrowingDisposeOperator());
        bool callbackInvoked = false;
        result.SetObserver(observer);
        result.SetDisposeCallback(
            () =>
            {
                callbackInvoked = true;
                return ValueTask.CompletedTask;
            });

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => result.DisposeAsync().AsTask());

        Assert.Equal("Operator disposal failed.", error.Message);
        Assert.True(callbackInvoked);
        Assert.Equal(1, observer.CompletionAttempts);
    }

    [Fact]
    public async Task AppendDisposeCallback_WhenExistingCallbackThrows_StillInvokesAppendedCallback()
    {
        var observer = new RecordingObserver();
        var result = new QueryResult(new NoOpOperator());
        bool appendedCallbackInvoked = false;
        result.SetObserver(observer);
        result.SetDisposeCallback(
            () => ValueTask.FromException(
                new InvalidOperationException("Existing callback failed.")));
        result.AppendDisposeCallback(
            () =>
            {
                appendedCallbackInvoked = true;
                return ValueTask.CompletedTask;
            });

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => result.DisposeAsync().AsTask());

        Assert.True(appendedCallbackInvoked);
        QueryResultCompletion completion = AssertCompletion(
            observer,
            QueryResultCompletionReason.Failed,
            0);
        Assert.Same(error, completion.Error);
    }

    private class NoOpOperator : IOperator
    {
        public ColumnDefinition[] OutputSchema { get; } = [];
        public bool ReusesCurrentRowBuffer => false;
        public DbValue[] Current => [];
        public virtual ValueTask OpenAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default) =>
            ValueTask.FromResult(false);
        public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ThrowingDisposeOperator : NoOpOperator
    {
        public override ValueTask DisposeAsync() =>
            ValueTask.FromException(
                new InvalidOperationException("Operator disposal failed."));
    }

    private sealed class ThrowingOpenOperator : NoOpOperator
    {
        public override ValueTask OpenAsync(CancellationToken ct = default) =>
            ValueTask.FromException(
                new InvalidOperationException("Operator open failed."));
    }

    private static QueryResultCompletion AssertCompletion(
        RecordingObserver observer,
        QueryResultCompletionReason reason,
        long rowsProduced)
    {
        QueryResultCompletion completion = Assert.Single(observer.Completions);
        Assert.Equal(reason, completion.Reason);
        Assert.Equal(rowsProduced, completion.RowsProduced);
        return completion;
    }

    private static List<DbValue[]> CreateRows(params long[] values)
        => values.Select(value => new[] { DbValue.FromInteger(value) }).ToList();

    private static RowBatch CreateBatch(params long[] values)
    {
        var batch = new RowBatch(columnCount: 1, capacity: values.Length);
        foreach (long value in values)
            batch.AppendRow([DbValue.FromInteger(value)]);
        return batch;
    }

    private sealed class RecordingObserver : IQueryResultObserver
    {
        private readonly object _gate = new();
        private readonly List<QueryResultCompletion> _completions = [];
        private int _firstRowCount;
        private int _rowCount;

        internal int FirstRowCount => Volatile.Read(ref _firstRowCount);
        internal int RowCount => Volatile.Read(ref _rowCount);

        internal QueryResultCompletion[] Completions
        {
            get
            {
                lock (_gate)
                    return [.. _completions];
            }
        }

        public void OnFirstRowProduced() => Interlocked.Increment(ref _firstRowCount);

        public void OnRowProduced() => Interlocked.Increment(ref _rowCount);

        public void OnCompleted(QueryResultCompletion completion)
        {
            lock (_gate)
                _completions.Add(completion);
        }
    }

    private sealed class ThrowingObserver : IQueryResultObserver
    {
        private int _firstRowAttempts;
        private int _rowAttempts;
        private int _completionAttempts;

        internal int FirstRowAttempts => Volatile.Read(ref _firstRowAttempts);
        internal int RowAttempts => Volatile.Read(ref _rowAttempts);
        internal int CompletionAttempts => Volatile.Read(ref _completionAttempts);

        public void OnFirstRowProduced()
        {
            Interlocked.Increment(ref _firstRowAttempts);
            throw new InvalidOperationException("First-row observer failed.");
        }

        public void OnRowProduced()
        {
            Interlocked.Increment(ref _rowAttempts);
            throw new InvalidOperationException("Row observer failed.");
        }

        public void OnCompleted(QueryResultCompletion completion)
        {
            Interlocked.Increment(ref _completionAttempts);
            throw new InvalidOperationException("Completion observer failed.");
        }
    }

    private sealed class ReentrantDisposalObserver : IQueryResultObserver
    {
        private Task _disposalTask = Task.CompletedTask;
        private int _disposalCompletedInsideCallback;

        internal QueryResult Result { get; set; } = null!;

        internal Task DisposalTask => Volatile.Read(ref _disposalTask);

        internal bool DisposalCompletedInsideCallback =>
            Volatile.Read(ref _disposalCompletedInsideCallback) != 0;

        public void OnFirstRowProduced()
        {
        }

        public void OnRowProduced()
        {
        }

        public void OnCompleted(QueryResultCompletion completion)
        {
            Task disposal = Task.Run(async () => await Result.DisposeAsync());
            Volatile.Write(ref _disposalTask, disposal);
            if (disposal.Wait(TimeSpan.FromSeconds(2)))
                Volatile.Write(ref _disposalCompletedInsideCallback, 1);
        }
    }

    private class SequenceOperator : IOperator
    {
        private readonly List<DbValue[]> _rows;
        private int _index = -1;

        internal SequenceOperator(params long[] values)
        {
            _rows = CreateRows(values);
        }

        public ColumnDefinition[] OutputSchema => SingleColumnSchema;
        public bool ReusesCurrentRowBuffer => false;
        public bool Opened { get; private set; }
        public bool Disposed { get; private set; }

        public DbValue[] Current => _index >= 0 && _index < _rows.Count
            ? _rows[_index]
            : throw new InvalidOperationException("No current row.");

        public ValueTask OpenAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            Opened = true;
            return ValueTask.CompletedTask;
        }

        public virtual ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            int nextIndex = _index + 1;
            if (nextIndex >= _rows.Count)
                return ValueTask.FromResult(false);

            _index = nextIndex;
            return ValueTask.FromResult(true);
        }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class FaultingOperator : SequenceOperator
    {
        private int _moveCount;

        internal FaultingOperator()
            : base(1)
        {
        }

        public override ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            if (Interlocked.Increment(ref _moveCount) > 1)
                return ValueTask.FromException<bool>(new InvalidOperationException("Move failed."));

            return base.MoveNextAsync(ct);
        }
    }

    private sealed class CurrentFaultOperator : IOperator
    {
        private bool _produced;

        public ColumnDefinition[] OutputSchema => SingleColumnSchema;
        public bool ReusesCurrentRowBuffer => false;
        public DbValue[] Current => throw new InvalidOperationException("Current failed.");

        public ValueTask OpenAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            if (_produced)
                return ValueTask.FromResult(false);

            _produced = true;
            return ValueTask.FromResult(true);
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private class SequenceBatchOperator : IBatchOperator
    {
        private readonly RowBatch[] _batches;
        private int _index;

        internal SequenceBatchOperator(params RowBatch[] batches)
        {
            _batches = batches;
            _index = -1;
        }

        public ColumnDefinition[] OutputSchema => SingleColumnSchema;
        public bool ReusesCurrentBatch => false;

        public RowBatch CurrentBatch => _index >= 0 && _index < _batches.Length
            ? _batches[_index]
            : throw new InvalidOperationException("No current batch.");

        public ValueTask OpenAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            _index = _batches.Length == 0 ? -1 : 0;
            return ValueTask.CompletedTask;
        }

        public virtual ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            int nextIndex = _index + 1;
            if (nextIndex >= _batches.Length)
                return ValueTask.FromResult(false);

            _index = nextIndex;
            return ValueTask.FromResult(true);
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FaultingBatchOperator : SequenceBatchOperator
    {
        internal FaultingBatchOperator(RowBatch initialBatch)
            : base(initialBatch)
        {
        }

        public override ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
            => ValueTask.FromException<bool>(new InvalidOperationException("Batch move failed."));
    }

    private sealed class BatchBackedOperator : IOperator, IBatchBackedRowOperator
    {
        internal BatchBackedOperator(IBatchOperator batchSource)
        {
            BatchSource = batchSource;
        }

        public IBatchOperator BatchSource { get; }
        public ColumnDefinition[] OutputSchema => BatchSource.OutputSchema;
        public bool ReusesCurrentRowBuffer => false;
        public DbValue[] Current => throw new NotSupportedException();
        public ValueTask OpenAsync(CancellationToken ct = default) => BatchSource.OpenAsync(ct);
        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default) =>
            throw new NotSupportedException();
        public ValueTask DisposeAsync() => BatchSource.DisposeAsync();
    }

    private sealed class MaterializedBatchOperator : IBatchOperator, IMaterializedRowsProvider
    {
        private readonly RowBatch _emptyBatch = new(columnCount: 1, capacity: 0);
        private List<DbValue[]>? _rows;

        internal MaterializedBatchOperator(List<DbValue[]> rows)
        {
            _rows = rows;
        }

        public ColumnDefinition[] OutputSchema => SingleColumnSchema;
        public bool ReusesCurrentBatch => false;
        public RowBatch CurrentBatch => _emptyBatch;

        public ValueTask OpenAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default) =>
            ValueTask.FromResult(false);

        public bool TryTakeMaterializedRows(out List<DbValue[]> rows)
        {
            rows = _rows ?? [];
            _rows = null;
            return true;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class BlockingExhaustionOperator : IOperator
    {
        private readonly TaskCompletionSource _entered =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _moveCount;

        internal Task ExhaustionEntered => _entered.Task;
        public ColumnDefinition[] OutputSchema => SingleColumnSchema;
        public bool ReusesCurrentRowBuffer => false;
        public DbValue[] Current { get; } = [DbValue.FromInteger(1)];

        public ValueTask OpenAsync(CancellationToken ct = default) => ValueTask.CompletedTask;

        public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        {
            if (Interlocked.Increment(ref _moveCount) == 1)
                return true;

            _entered.TrySetResult();
            await _release.Task.WaitAsync(ct);
            return false;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        internal void ReleaseExhaustion() => _release.TrySetResult();
    }
}
