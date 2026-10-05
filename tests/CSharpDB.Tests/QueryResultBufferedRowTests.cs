using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class QueryResultBufferedRowTests
{
    private static CancellationToken TestToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task MoveNext_TraversesEmptyBatchesAndBoundariesExactlyOnce()
    {
        var source = new BatchSource(0, 64, 0, 64, 2, 0);
        await using var result = QueryResult.FromBatchOperator(source);
        for (int expected = 1; expected <= 130; expected++)
        {
            Assert.True(await result.MoveNextAsync(TestToken));
            AssertRow(result.Current, expected);
        }

        Assert.False(await result.MoveNextAsync(TestToken));
        int getters = source.GetterCalls;
        int refills = source.RefillCalls;
        Assert.False(await result.MoveNextAsync(TestToken));
        Assert.Equal(getters, source.GetterCalls);
        Assert.Equal(refills, source.RefillCalls);
        Assert.Throws<InvalidOperationException>(() => result.Current);
        Assert.Equal(1, source.OpenCalls);
    }

    [Fact]
    public async Task MoveNext_ReadsCurrentBatchOncePerBufferedRowAndOnceAfterRefill()
    {
        var source = new BatchSource(2, 2);
        await using var result = QueryResult.FromBatchOperator(source);
        Assert.Equal(0, source.GetterCalls);
        foreach (int expectedCalls in new[] { 2, 3, 5, 6 })
        {
            Assert.True(await result.MoveNextAsync(TestToken));
            Assert.Equal(expectedCalls, source.GetterCalls);
        }
        Assert.False(await result.MoveNextAsync(TestToken));
        Assert.Equal(7, source.GetterCalls);
        Assert.Equal(3, source.RefillCalls);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task MoveNext_AwaitsSuspendedOpenOrRefill(bool opening)
    {
        var source = new BatchSource(1, 1);
        var gate = NewGate();
        await using var result = QueryResult.FromBatchOperator(source);
        if (opening)
            source.OpenGate = gate;
        else
        {
            Assert.True(await result.MoveNextAsync(TestToken));
            source.RefillGate = gate;
        }

        ValueTask<bool> pending = result.MoveNextAsync(TestToken);
        Assert.False(pending.IsCompleted);
        gate.SetResult(true);
        Assert.True(await pending.AsTask().WaitAsync(TimeSpan.FromSeconds(10), TestToken));
        AssertRow(result.Current, opening ? 1 : 2);
        Assert.Equal(1, source.OpenCalls);
    }

    [Fact]
    public async Task BufferedGetterFault_IsDeliveredAsFaultedAwaitableAndRetryKeepsNextRow()
    {
        var source = new BatchSource(2);
        await using var result = QueryResult.FromBatchOperator(source);
        Assert.True(await result.MoveNextAsync(TestToken));
        var error = new IOException("controlled current batch failure");
        source.GetterError = error;

        ValueTask<bool> move = result.MoveNextAsync(TestToken);
        Task<bool> task = move.AsTask();
        Assert.Same(error, await Assert.ThrowsAsync<IOException>(() => task));
        Assert.True(task.IsFaulted);
        AssertRow(result.Current, 1);
        Assert.True(await result.MoveNextAsync(TestToken));
        AssertRow(result.Current, 2);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task BufferedGetterCancellation_PreservesExceptionAndCanceledTask(bool cancelToken)
    {
        using var cancellation = new CancellationTokenSource();
        if (cancelToken)
            cancellation.Cancel();
        var source = new BatchSource(2);
        await using var result = QueryResult.FromBatchOperator(source);
        Assert.True(await result.MoveNextAsync(TestToken));
        var error = new OperationCanceledException("controlled cancellation", cancellation.Token);
        source.GetterError = error;

        ValueTask<bool> move = result.MoveNextAsync(TestToken);
        Task<bool> task = move.AsTask();
        Assert.Same(error, await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task));
        Assert.True(task.IsCanceled);
        Assert.True(await result.MoveNextAsync(TestToken));
        AssertRow(result.Current, 2);
    }

    [Fact]
    public async Task CanceledCallerToken_AllowsBufferedRowButCancelsRefillAndCanRetry()
    {
        var source = new BatchSource(2, 1);
        await using var result = QueryResult.FromBatchOperator(source);
        Assert.True(await result.MoveNextAsync(TestToken));
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.True(await result.MoveNextAsync(cancellation.Token));
        AssertRow(result.Current, 2);
        Task<bool> task = result.MoveNextAsync(cancellation.Token).AsTask();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
        Assert.True(task.IsCanceled);
        Assert.True(await result.MoveNextAsync(TestToken));
        AssertRow(result.Current, 3);
        Assert.False(await result.MoveNextAsync(TestToken));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SuspendedRefill_CancellationOrFaultAllowsRetryWithoutSkipping(bool cancel)
    {
        var source = new BatchSource(1, 1);
        await using var result = QueryResult.FromBatchOperator(source);
        Assert.True(await result.MoveNextAsync(TestToken));
        var gate = source.RefillGate = NewGate();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        Task<bool> task = result.MoveNextAsync(cancellation.Token).AsTask();
        Assert.False(task.IsCompleted);
        if (cancel)
        {
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => task.WaitAsync(TimeSpan.FromSeconds(10), TestToken));
            Assert.True(task.IsCanceled);
            gate.SetResult(true);
        }
        else
        {
            var error = new IOException("controlled refill failure");
            gate.SetException(error);
            Assert.Same(error, await Assert.ThrowsAsync<IOException>(
                () => task.WaitAsync(TimeSpan.FromSeconds(10), TestToken)));
        }

        AssertRow(result.Current, 1);
        Assert.True(await result.MoveNextAsync(TestToken));
        AssertRow(result.Current, 2);
        Assert.False(await result.MoveNextAsync(TestToken));
    }

    [Fact]
    public async Task CurrentReusesWorkingRow_EnumerationRetainsIndependentRowsAcrossRefillsAndDisposal()
    {
        var source = new BatchSource(2, 0, 2, 1);
        var result = QueryResult.FromBatchOperator(source);
        List<DbValue[]> retained = [];
        await using (result)
        {
            Assert.True(await result.MoveNextAsync(TestToken));
            DbValue[] working = result.Current;
            Assert.True(await result.MoveNextAsync(TestToken));
            Assert.Same(working, result.Current);
            AssertRow(working, 2);
            await foreach (DbValue[] row in result.GetRowsAsync(TestToken))
            {
                Assert.NotSame(working, row);
                retained.Add(row);
            }
        }

        Assert.Equal(3, retained.Count);
        for (int i = 0; i < retained.Count; i++)
            AssertRow(retained[i], i + 3);
        Assert.NotSame(retained[0], retained[1]);
        Assert.NotSame(retained[1], retained[2]);
        Assert.Equal(1, source.DisposeCalls);
    }

    [Theory]
    [InlineData(63)]
    [InlineData(64)]
    [InlineData(65)]
    public async Task PartialToList_ReturnsOnlyRemainingOwnedRowsAcrossBoundary(int consumed)
    {
        var source = new BatchSource(64, 0, 64, 2);
        var result = QueryResult.FromBatchOperator(source);
        List<DbValue[]> rows;
        await using (result)
        {
            for (int i = 1; i <= consumed; i++)
            {
                Assert.True(await result.MoveNextAsync(TestToken));
                AssertRow(result.Current, i);
            }
            DbValue[] working = result.Current;
            rows = await result.ToListAsync(TestToken);
            Assert.Equal(130 - consumed, rows.Count);
            Assert.All(rows, row => Assert.NotSame(working, row));
            Assert.False(await result.MoveNextAsync(TestToken));
            Assert.Empty(await result.ToListAsync(TestToken));
            Assert.Throws<InvalidOperationException>(() => result.Current);
        }
        for (int i = 0; i < rows.Count; i++)
        {
            AssertRow(rows[i], consumed + i + 1);
            if (i > 0)
                Assert.NotSame(rows[i - 1], rows[i]);
        }
    }

    [Fact]
    public async Task LateExecutionScope_AppliesToSubsequentRefillAndDisposal()
    {
        var source = new BatchSource(2, 1);
        var result = QueryResult.FromBatchOperator(source);
        int depth = 0;
        int entered = 0;
        int exited = 0;
        await using (result)
        {
            Assert.True(await result.MoveNextAsync(TestToken));
            result.SetExecutionScopeFactory(() =>
            {
                entered++;
                depth++;
                return new CallbackScope(() => { depth--; exited++; });
            });
            source.OnRefill = () => Assert.Equal(1, depth);
            source.OnDispose = () => Assert.Equal(1, depth);
            Assert.True(await result.MoveNextAsync(TestToken));
            AssertRow(result.Current, 2);
            Assert.Equal(0, entered);
            Assert.True(await result.MoveNextAsync(TestToken));
            AssertRow(result.Current, 3);
            Assert.Equal(1, entered);
            Assert.Equal(0, depth);
        }
        Assert.Equal(2, entered);
        Assert.Equal(entered, exited);
        Assert.Equal(0, depth);
    }

    private static void AssertRow(DbValue[] row, int expected)
    {
        Assert.Equal(2, row.Length);
        Assert.Equal(expected, row[0].AsInteger);
        Assert.Equal(expected * 10L, row[1].AsInteger);
    }

    private static TaskCompletionSource<bool> NewGate() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class CallbackScope(Action callback) : IDisposable
    {
        public void Dispose() => callback();
    }

    private sealed class BatchSource(params int[] sizes) : IBatchOperator
    {
        private readonly RowBatch _batch = new(2, Math.Max(1, sizes.Max()));
        private int _nextBatch;
        private int _nextValue = 1;
        public ColumnDefinition[] OutputSchema { get; } =
        [
            new() { Name = "id", Type = DbType.Integer },
            new() { Name = "value", Type = DbType.Integer },
        ];
        public TaskCompletionSource<bool>? OpenGate { get; set; }
        public TaskCompletionSource<bool>? RefillGate { get; set; }
        public Exception? GetterError { get; set; }
        public Action? OnRefill { get; set; }
        public Action? OnDispose { get; set; }
        public int GetterCalls { get; private set; }
        public int OpenCalls { get; private set; }
        public int RefillCalls { get; private set; }
        public int DisposeCalls { get; private set; }

        public RowBatch CurrentBatch
        {
            get
            {
                GetterCalls++;
                if (GetterError is { } error)
                {
                    GetterError = null;
                    throw error;
                }
                return _batch;
            }
        }

        public async ValueTask OpenAsync(CancellationToken ct = default)
        {
            OpenCalls++;
            ct.ThrowIfCancellationRequested();
            var gate = OpenGate;
            OpenGate = null;
            if (gate is not null)
                await gate.Task.WaitAsync(ct);
            _batch.Reset();
            _nextBatch = 0;
            _nextValue = 1;
        }

        public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
        {
            RefillCalls++;
            OnRefill?.Invoke();
            ct.ThrowIfCancellationRequested();
            var gate = RefillGate;
            RefillGate = null;
            if (gate is not null)
                await gate.Task.WaitAsync(ct);
            if (_nextBatch == sizes.Length)
                return false;
            _batch.Reset();
            int count = sizes[_nextBatch++];
            for (int i = 0; i < count; i++)
            {
                int value = _nextValue++;
                _batch.AppendRow([DbValue.FromInteger(value), DbValue.FromInteger(value * 10L)]);
            }
            return true;
        }

        public ValueTask DisposeAsync()
        {
            DisposeCalls++;
            OnDispose?.Invoke();
            return ValueTask.CompletedTask;
        }
    }
}
