using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class LimitBatchForwardingTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(true, 17)]
    [InlineData(false, 17)]
    [InlineData(true, 64)]
    [InlineData(false, 64)]
    [InlineData(true, 63)]
    [InlineData(false, 63)]
    public async Task UntouchedSourceBatchThatFits_IsForwarded(bool sourceReusesBatch, int batchSize)
    {
        var source = new CursorBatchSource(batchSize * 2, sourceReusesBatch, batchSize);
        await using var limit = new LimitOperator(source, batchSize * 2);
        var batches = (IBatchOperator)limit;
        await limit.OpenAsync(Ct);

        for (int first = 0; first < batchSize * 2; first += batchSize)
        {
            Assert.True(await batches.MoveNextBatchAsync(Ct));
            Assert.Same(source.CurrentBatch, batches.CurrentBatch);
            AssertRows(batches.CurrentBatch, first, batchSize);
        }
        Assert.False(await batches.MoveNextBatchAsync(Ct));
    }

    [Theory]
    [InlineData(64, true)]
    [InlineData(65, true)]
    [InlineData(128, true)]
    [InlineData(130, true)]
    [InlineData(64, false)]
    [InlineData(65, false)]
    [InlineData(128, false)]
    [InlineData(130, false)]
    public async Task LimitAtBatchBoundaries_ReturnsExactlyTheRequestedRows(int rowLimit, bool sourceReusesBatch)
    {
        var source = new CursorBatchSource(200, sourceReusesBatch, 64);
        await using var limit = new LimitOperator(source, rowLimit);
        await limit.OpenAsync(Ct);

        long[] rows = await DrainAsync((IBatchOperator)limit);
        Assert.Equal(Enumerable.Range(0, rowLimit).Select(value => (long)value), rows);
        Assert.False(await limit.MoveNextAsync(Ct));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CurrentBatchObtainedBeforeAdvancing_RemainsTheFirstOutputBuffer(bool sourceReusesBatch)
    {
        var source = new CursorBatchSource(128, sourceReusesBatch, 64);
        await using var limit = new LimitOperator(source, 128);
        var batches = (IBatchOperator)limit;
        await limit.OpenAsync(Ct);
        RowBatch empty = batches.CurrentBatch;
        Assert.Equal(0, empty.Count);

        Assert.True(await batches.MoveNextBatchAsync(Ct));
        Assert.Same(empty, batches.CurrentBatch);
        Assert.NotSame(source.CurrentBatch, batches.CurrentBatch);
        AssertRows(batches.CurrentBatch, 0, 64);
        Assert.Equal(Enumerable.Range(64, 64).Select(value => (long)value), await DrainAsync(batches));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task LargeSourceBatches_CarryCopiedRemaindersAcrossOutputBoundaries(bool sourceReusesBatch)
    {
        var source = new CursorBatchSource(350, sourceReusesBatch, 130);
        await using var limit = new LimitOperator(source, 260);
        var batches = (IBatchOperator)limit;
        await limit.OpenAsync(Ct);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        Assert.NotSame(source.CurrentBatch, batches.CurrentBatch);
        AssertRows(batches.CurrentBatch, 0, 64);
        Assert.Equal(Enumerable.Range(64, 196).Select(value => (long)value), await DrainAsync(batches));
        Assert.False(await limit.MoveNextAsync(Ct));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ReuseDisabled_RetainedOutputsSurviveSubsequentReadsAndReopen(bool sourceReusesBatch)
    {
        var source = new CursorBatchSource(200, sourceReusesBatch, 64);
        await using var limit = new LimitOperator(source, 130);
        var batches = (IBatchOperator)limit;
        await limit.OpenAsync(Ct);
        limit.SetReuseCurrentBatch(false);
        Assert.False(batches.ReusesCurrentBatch);
        var retained = new List<(RowBatch Batch, int First, int Count)>();
        int next = 0;
        while (await batches.MoveNextBatchAsync(Ct))
        {
            RowBatch output = batches.CurrentBatch;
            Assert.DoesNotContain(retained, previous => ReferenceEquals(previous.Batch, output));
            if (sourceReusesBatch) Assert.NotSame(source.CurrentBatch, output);
            else if (output.Count == 64) Assert.Same(source.CurrentBatch, output);
            AssertRows(output, next, output.Count);
            retained.Add((output, next, output.Count));
            next += output.Count;
        }
        Assert.Equal(130, next);

        await limit.DisposeAsync();
        await limit.OpenAsync(Ct);
        limit.SetReuseCurrentBatch(true);
        Assert.Equal(Enumerable.Range(0, 130).Select(value => (long)value), await DrainAsync(batches));
        foreach (var previous in retained)
            AssertRows(previous.Batch, previous.First, previous.Count);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ReuseTrueFalseTrue_DoesNotMutateOutputRetainedWhileReuseWasDisabled(bool sourceReusesBatch)
    {
        var source = new CursorBatchSource(256, sourceReusesBatch, 64);
        await using var limit = new LimitOperator(source, 256);
        var batches = (IBatchOperator)limit;
        await limit.OpenAsync(Ct);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        Assert.Same(source.CurrentBatch, batches.CurrentBatch);
        AssertRows(batches.CurrentBatch, 0, 64);

        limit.SetReuseCurrentBatch(false);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        RowBatch retained = batches.CurrentBatch;
        AssertRows(retained, 64, 64);
        limit.SetReuseCurrentBatch(true);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        Assert.NotSame(retained, batches.CurrentBatch);
        AssertRows(batches.CurrentBatch, 128, 64);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        Assert.NotSame(retained, batches.CurrentBatch);
        AssertRows(batches.CurrentBatch, 192, 64);
        Assert.False(await batches.MoveNextBatchAsync(Ct));
        AssertRows(retained, 64, 64);
    }

    [Fact]
    public async Task ReopenAfterForwarding_RestartsAtTheFirstRow()
    {
        var source = new CursorBatchSource(200, reuseBatch: true, 64);
        await using var limit = new LimitOperator(source, 130);
        var batches = (IBatchOperator)limit;
        await limit.OpenAsync(Ct);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        Assert.Same(source.CurrentBatch, batches.CurrentBatch);
        AssertRows(batches.CurrentBatch, 0, 64);

        await limit.OpenAsync(Ct);
        Assert.Equal(Enumerable.Range(0, 130).Select(value => (long)value), await DrainAsync(batches));
    }

    [Fact]
    public async Task ZeroLimit_DoesNotAdvanceTheSource()
    {
        var source = new CursorBatchSource(10, reuseBatch: true, 64);
        await using var limit = new LimitOperator(source, 0);
        var batches = (IBatchOperator)limit;
        await limit.OpenAsync(Ct);
        Assert.False(await batches.MoveNextBatchAsync(Ct));
        Assert.False(await limit.MoveNextAsync(Ct));
        Assert.Equal(0, batches.CurrentBatch.Count);
        Assert.Equal(0, source.BatchReadCalls);
        Assert.Equal(0, source.RowReadCalls);
    }

    [Fact]
    public async Task EmptySource_DoesNotReturnAnEmptySuccessfulBatch()
    {
        var source = new CursorBatchSource(0, reuseBatch: true, 64);
        await using var limit = new LimitOperator(source, 130);
        var batches = (IBatchOperator)limit;
        await limit.OpenAsync(Ct);
        Assert.False(await batches.MoveNextBatchAsync(Ct));
        Assert.Equal(0, batches.CurrentBatch.Count);
    }

    [Fact]
    public async Task EmptyIntermediateSourceBatch_DoesNotEndTheRead()
    {
        var source = new CursorBatchSource(34, reuseBatch: true, 17, 0, 17);
        await using var limit = new LimitOperator(source, 34);
        var batches = (IBatchOperator)limit;
        await limit.OpenAsync(Ct);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        AssertRows(batches.CurrentBatch, 0, 17);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        Assert.Same(source.CurrentBatch, batches.CurrentBatch);
        AssertRows(batches.CurrentBatch, 17, 17);
        Assert.Equal(3, source.BatchReadCalls);
        Assert.False(await batches.MoveNextBatchAsync(Ct));
    }

    [Fact]
    public async Task AsynchronouslyCompletedSourceBatch_IsForwarded()
    {
        var source = new CursorBatchSource(64, reuseBatch: true, 64) { CompleteAsynchronously = true };
        await using var limit = new LimitOperator(source, 64);
        var batches = (IBatchOperator)limit;
        await limit.OpenAsync(Ct);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        Assert.Same(source.CurrentBatch, batches.CurrentBatch);
        AssertRows(batches.CurrentBatch, 0, 64);
        Assert.False(await batches.MoveNextBatchAsync(Ct));
    }

    [Fact]
    public async Task CanceledAsynchronousBatchRead_DoesNotConsumeRows()
    {
        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = new CursorBatchSource(64, reuseBatch: true, 64)
        {
            CompleteAsynchronously = true,
            AsyncBatchGate = gate,
        };
        await using var limit = new LimitOperator(source, 64);
        var batches = (IBatchOperator)limit;
        await limit.OpenAsync(Ct);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        ValueTask<bool> pending = batches.MoveNextBatchAsync(cancellation.Token);
        await source.AsyncBatchStarted.Task.WaitAsync(Ct);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending.AsTask());

        gate.SetResult(true);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        AssertRows(batches.CurrentBatch, 0, 64);
        Assert.False(await batches.MoveNextBatchAsync(Ct));
    }

    [Fact]
    public async Task RowReadsBetweenFullyForwardedBatches_PreserveCursorOrder()
    {
        var source = new CursorBatchSource(300, reuseBatch: true, 64);
        await using var limit = new LimitOperator(source, 194);
        var batches = (IBatchOperator)limit;
        await limit.OpenAsync(Ct);
        Assert.True(await limit.MoveNextAsync(Ct));
        AssertRow(limit.Current, 0);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        Assert.Same(source.CurrentBatch, batches.CurrentBatch);
        AssertRows(batches.CurrentBatch, 1, 64);
        Assert.True(await limit.MoveNextAsync(Ct));
        AssertRow(limit.Current, 65);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        Assert.Same(source.CurrentBatch, batches.CurrentBatch);
        AssertRows(batches.CurrentBatch, 66, 64);
        Assert.True(await limit.MoveNextAsync(Ct));
        AssertRow(limit.Current, 130);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        Assert.NotSame(source.CurrentBatch, batches.CurrentBatch);
        AssertRows(batches.CurrentBatch, 131, 63);
        Assert.False(await limit.MoveNextAsync(Ct));
        Assert.False(await batches.MoveNextBatchAsync(Ct));
    }

    [Fact]
    public async Task RowReadsAfterAnOwnedCopyAndForwardedBatch_PreserveCursorOrder()
    {
        var source = new CursorBatchSource(300, reuseBatch: true, 64);
        await using var limit = new LimitOperator(source, 194);
        var batches = (IBatchOperator)limit;
        await limit.OpenAsync(Ct);
        RowBatch owned = batches.CurrentBatch;
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        Assert.Same(owned, batches.CurrentBatch);
        Assert.NotSame(source.CurrentBatch, batches.CurrentBatch);
        AssertRows(batches.CurrentBatch, 0, 64);

        Assert.True(await limit.MoveNextAsync(Ct));
        AssertRow(limit.Current, 64);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        AssertRows(batches.CurrentBatch, 65, 64);
        Assert.True(await limit.MoveNextAsync(Ct));
        AssertRow(limit.Current, 129);
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        AssertRows(batches.CurrentBatch, 130, 64);
        Assert.False(await limit.MoveNextAsync(Ct));
        Assert.False(await batches.MoveNextBatchAsync(Ct));
    }

    private static async Task<long[]> DrainAsync(IBatchOperator batches)
    {
        var ids = new List<long>();
        while (await batches.MoveNextBatchAsync(Ct))
        {
            RowBatch batch = batches.CurrentBatch;
            Assert.True(batch.Count > 0);
            for (int row = 0; row < batch.Count; row++)
            {
                long id = batch.GetRowSpan(row)[0].AsInteger;
                Assert.Equal(-id - 1, batch.GetRowSpan(row)[1].AsInteger);
                ids.Add(id);
            }
        }
        return ids.ToArray();
    }

    private static void AssertRows(RowBatch batch, int first, int count)
    {
        Assert.Equal(2, batch.ColumnCount);
        Assert.Equal(count, batch.Count);
        for (int row = 0; row < count; row++) AssertRow(batch.GetRowSpan(row), first + row);
    }

    private static void AssertRow(ReadOnlySpan<DbValue> row, int id)
    {
        Assert.Equal(2, row.Length);
        Assert.Equal((long)id, row[0].AsInteger);
        Assert.Equal(-(long)id - 1, row[1].AsInteger);
    }

    // Row reads advance the underlying cursor without changing CurrentBatch, as
    // PrimaryKeyRangeScanOperator does. This catches stale-batch forwarding.
    private sealed class CursorBatchSource(int rowCount, bool reuseBatch, params int[] batchSizes) : IOperator, IBatchOperator
    {
        private int _next;
        private int _batchNumber;
        private RowBatch? _currentBatch;

        public ColumnDefinition[] OutputSchema { get; } =
        [
            new() { Name = "id", Type = DbType.Integer },
            new() { Name = "value", Type = DbType.Integer },
        ];
        public bool ReusesCurrentRowBuffer => false;
        public bool ReusesCurrentBatch => reuseBatch;
        public DbValue[] Current { get; private set; } = [];
        public RowBatch CurrentBatch => _currentBatch ??= new RowBatch(2, 0);
        public int BatchReadCalls { get; private set; }
        public int RowReadCalls { get; private set; }
        public bool CompleteAsynchronously { get; init; }
        public TaskCompletionSource<bool>? AsyncBatchGate { get; init; }
        public TaskCompletionSource<bool> AsyncBatchStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask OpenAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            _next = 0;
            _batchNumber = 0;
            _currentBatch = null;
            Current = [];
            BatchReadCalls = 0;
            RowReadCalls = 0;
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            RowReadCalls++;
            if (_next >= rowCount) return ValueTask.FromResult(false);
            Current = MakeRow(_next++);
            return ValueTask.FromResult(true);
        }

        public ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            BatchReadCalls++;
            return CompleteAsynchronously ? MoveNextBatchAfterYieldAsync(ct) : ValueTask.FromResult(MoveNextBatchCore());
        }

        private async ValueTask<bool> MoveNextBatchAfterYieldAsync(CancellationToken ct)
        {
            AsyncBatchStarted.TrySetResult(true);
            if (AsyncBatchGate is { } gate) await gate.Task.WaitAsync(ct);
            else await Task.Yield();
            ct.ThrowIfCancellationRequested();
            return MoveNextBatchCore();
        }

        private bool MoveNextBatchCore()
        {
            if (_next >= rowCount)
            {
                _currentBatch = null;
                return false;
            }

            int capacity = batchSizes[_batchNumber++ % batchSizes.Length];
            RowBatch batch = reuseBatch && _currentBatch is { } current && current.Capacity == capacity
                ? current
                : new RowBatch(2, capacity);
            batch.Reset();
            while (batch.Count < batch.Capacity && _next < rowCount)
                batch.AppendRow(MakeRow(_next++));
            _currentBatch = batch;
            return true;
        }

        public ValueTask DisposeAsync()
        {
            _currentBatch = null;
            Current = [];
            return ValueTask.CompletedTask;
        }

        private static DbValue[] MakeRow(int id) => [DbValue.FromInteger(id), DbValue.FromInteger(-(long)id - 1)];
    }
}
