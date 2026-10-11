using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class LimitMixedTraversalTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(65)]
    [InlineData(128)]
    [InlineData(130)]
    public async Task RowAfterPartialBatch_ReadsBufferedRowsBeforeSourceEof(int sourceBatchSize)
    {
        var source = new CursorSource(sourceBatchSize, sourceBatchSize);
        await using var limit = new LimitOperator(source, sourceBatchSize);
        await limit.OpenAsync(Ct);
        var ids = new List<long>();

        await ReadBatchAsync(limit, ids);
        Assert.Equal(64, ids.Count);
        while (ids.Count < sourceBatchSize)
            await ReadRowAsync(limit, ids);

        AssertSequence(ids, sourceBatchSize);
        await AssertEofAsync(limit);
    }

    [Theory]
    [InlineData(false, 65, true)]
    [InlineData(false, 128, true)]
    [InlineData(false, 130, true)]
    [InlineData(true, 64, true)]
    [InlineData(true, 65, true)]
    [InlineData(true, 128, true)]
    [InlineData(true, 130, true)]
    [InlineData(false, 130, false)]
    [InlineData(true, 130, false)]
    public async Task AlternatingBatchesAndRows_RespectsOrderAndFiniteLimit(
        bool throughAdapter, int sourceBatchSize, bool sourceReusesBatch)
    {
        var source = new CursorSource(400, sourceBatchSize, sourceReusesBatch);
        await using var limit = new LimitOperator(AsRowSource(source, throughAdapter), 197);
        await limit.OpenAsync(Ct);
        var ids = new List<long>();

        while (ids.Count < 197)
        {
            await ReadBatchAsync(limit, ids);
            if (ids.Count < 197)
                await ReadRowAsync(limit, ids);
        }

        AssertSequence(ids, 197);
        await AssertEofAsync(limit);
    }

    [Theory]
    [InlineData(64)]
    [InlineData(65)]
    [InlineData(130)]
    public async Task AdapterRowsBeforeBatch_DoNotReplayAlreadyConsumedRows(int sourceBatchSize)
    {
        var source = new CursorSource(250, sourceBatchSize);
        await using var limit = new LimitOperator(new BatchToRowOperatorAdapter(source), 135);
        await limit.OpenAsync(Ct);
        var ids = new List<long>();

        await ReadRowAsync(limit, ids);
        await ReadRowAsync(limit, ids);
        await ReadBatchAsync(limit, ids);
        await ReadRowAsync(limit, ids);
        await DrainBatchesAsync(limit, ids);

        AssertSequence(ids, 135);
        await AssertEofAsync(limit);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task SmallAdapterLimit_RowThenBatchSharesOneCursor(int rowLimit)
    {
        var source = new CursorSource(100, 65);
        await using var limit = new LimitOperator(new BatchToRowOperatorAdapter(source), rowLimit);
        await limit.OpenAsync(Ct);
        var ids = new List<long>();

        await ReadRowAsync(limit, ids);
        if (rowLimit > 1)
            await ReadBatchAsync(limit, ids);

        AssertSequence(ids, rowLimit);
        await AssertEofAsync(limit);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReuseDisabled_RetainedRowsAndBatchesSurviveMixedTraversal(bool throughAdapter)
    {
        var source = new CursorSource(350, 130);
        await using var limit = new LimitOperator(AsRowSource(source, throughAdapter), 260);
        limit.SetReuseCurrentRowBuffer(false);
        limit.SetReuseCurrentBatch(false);
        await limit.OpenAsync(Ct);
        Assert.False(limit.ReusesCurrentRowBuffer);
        Assert.False(((IBatchOperator)limit).ReusesCurrentBatch);
        var ids = new List<long>();

        await ReadBatchAsync(limit, ids);
        RowBatch retainedBatch = ((IBatchOperator)limit).CurrentBatch;
        await ReadRowAsync(limit, ids);
        DbValue[] row64 = limit.Current;
        await ReadRowAsync(limit, ids);
        DbValue[] row65 = limit.Current;
        Assert.NotSame(row64, row65);
        await ReadBatchAsync(limit, ids);
        await ReadRowAsync(limit, ids);
        await DrainBatchesAsync(limit, ids);

        AssertSequence(ids, 260);
        AssertRow(row64, 64);
        AssertRow(row65, 65);
        AssertRows(retainedBatch, 0, 64);
        await AssertEofAsync(limit);
        AssertRow(row64, 64);
        AssertRow(row65, 65);
        AssertRows(retainedBatch, 0, 64);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReuseEnabledAfterOwnedRow_DoesNotMutateThatRetainedRow(bool throughAdapter)
    {
        var source = new CursorSource(350, 130);
        await using var limit = new LimitOperator(AsRowSource(source, throughAdapter), 200);
        await limit.OpenAsync(Ct);
        var ids = new List<long>();
        await ReadBatchAsync(limit, ids);
        await ReadRowAsync(limit, ids);

        limit.SetReuseCurrentRowBuffer(false);
        await ReadRowAsync(limit, ids);
        DbValue[] retained = limit.Current;
        limit.SetReuseCurrentRowBuffer(true);
        await ReadRowAsync(limit, ids);
        await ReadBatchAsync(limit, ids);
        await ReadRowAsync(limit, ids);
        await DrainBatchesAsync(limit, ids);

        AssertSequence(ids, 200);
        AssertRow(retained, 65);
        await AssertEofAsync(limit);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReopenWithBufferedRows_RestartsBothTraversalModes(bool throughAdapter)
    {
        var source = new CursorSource(250, 130);
        await using var limit = new LimitOperator(AsRowSource(source, throughAdapter), 165);
        await limit.OpenAsync(Ct);
        var abandoned = new List<long>();
        await ReadBatchAsync(limit, abandoned);
        await ReadRowAsync(limit, abandoned);

        await limit.OpenAsync(Ct);
        var ids = new List<long>();
        await ReadRowAsync(limit, ids);
        await ReadBatchAsync(limit, ids);
        await ReadRowAsync(limit, ids);
        await DrainBatchesAsync(limit, ids);

        AssertSequence(ids, 165);
        await AssertEofAsync(limit);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CanceledRefillAfterMixedReads_DoesNotConsumeRows(bool throughAdapter)
    {
        var source = new CursorSource(200, 65);
        await using var limit = new LimitOperator(AsRowSource(source, throughAdapter), 130);
        await limit.OpenAsync(Ct);
        var ids = new List<long>();
        await ReadBatchAsync(limit, ids);
        await ReadRowAsync(limit, ids);
        Assert.Equal(65, ids.Count);

        Task started = source.BlockNextBatchRead();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        ValueTask<bool> pending = ((IBatchOperator)limit).MoveNextBatchAsync(cancellation.Token);
        await started.WaitAsync(Ct);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending.AsTask());

        await ReadRowAsync(limit, ids);
        await DrainBatchesAsync(limit, ids);
        AssertSequence(ids, 130);
        await AssertEofAsync(limit);

        await limit.OpenAsync(Ct);
        var reopened = new List<long>();
        await ReadRowAsync(limit, reopened);
        await ReadBatchAsync(limit, reopened);
        AssertSequence(reopened, reopened.Count);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SuspendedDirectRowFailure_RetryDoesNotConsumeTheLimit(bool cancel)
    {
        var source = new CursorSource(6, 65);
        await using var limit = new LimitOperator(source, 4);
        await limit.OpenAsync(Ct);
        TaskCompletionSource<bool> gate = source.SuspendNextRowRead();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);

        ValueTask<bool> pending = limit.MoveNextAsync(cancellation.Token);
        Assert.False(pending.IsCompleted);
        await source.RowReadStarted.Task.WaitAsync(Ct);
        if (cancel)
        {
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending.AsTask());
            gate.SetResult(true);
        }
        else
        {
            var failure = new InvalidOperationException("Injected direct-row failure.");
            gate.SetException(failure);
            Assert.Same(failure,
                await Assert.ThrowsAsync<InvalidOperationException>(() => pending.AsTask()));
        }

        var ids = new List<long>();
        for (int i = 0; i < 4; i++)
            await ReadRowAsync(limit, ids);

        AssertSequence(ids, 4);
        Assert.False(await limit.MoveNextAsync(Ct));
        Assert.Equal(5, source.RowReadCalls); // One failed attempt and four successful reads.
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public async Task DirectSourceEndsBeforeLimit_ReopenRestartsTheRows(int sourceRowCount)
    {
        var source = new CursorSource(sourceRowCount, 65);
        await using var limit = new LimitOperator(source, 4);

        for (int pass = 0; pass < 2; pass++)
        {
            await limit.OpenAsync(Ct);
            var ids = new List<long>();
            for (int i = 0; i < sourceRowCount; i++)
                await ReadRowAsync(limit, ids);

            AssertSequence(ids, sourceRowCount);
            Assert.False(await limit.MoveNextAsync(Ct));
            Assert.False(await limit.MoveNextAsync(Ct));
        }
    }

    private static IOperator AsRowSource(CursorSource source, bool throughAdapter)
        => throughAdapter ? new BatchToRowOperatorAdapter(source) : source;

    private static async Task ReadRowAsync(LimitOperator limit, List<long> ids)
    {
        Assert.True(await limit.MoveNextAsync(Ct));
        AssertRow(limit.Current, ids.Count);
        ids.Add(limit.Current[0].AsInteger);
    }

    private static async Task ReadBatchAsync(LimitOperator limit, List<long> ids)
    {
        var batches = (IBatchOperator)limit;
        Assert.True(await batches.MoveNextBatchAsync(Ct));
        AppendBatch(batches.CurrentBatch, ids);
    }

    private static async Task DrainBatchesAsync(LimitOperator limit, List<long> ids)
    {
        var batches = (IBatchOperator)limit;
        while (await batches.MoveNextBatchAsync(Ct))
            AppendBatch(batches.CurrentBatch, ids);
    }

    private static void AppendBatch(RowBatch batch, List<long> ids)
    {
        Assert.InRange(batch.Count, 1, 64);
        for (int i = 0; i < batch.Count; i++)
        {
            AssertRow(batch.GetRowSpan(i), ids.Count);
            ids.Add(batch.GetRowSpan(i)[0].AsInteger);
        }
    }

    private static async Task AssertEofAsync(LimitOperator limit)
    {
        Assert.False(await limit.MoveNextAsync(Ct));
        Assert.False(await ((IBatchOperator)limit).MoveNextBatchAsync(Ct));
        Assert.False(await limit.MoveNextAsync(Ct));
    }

    private static void AssertSequence(List<long> ids, int count)
        => Assert.Equal(Enumerable.Range(0, count).Select(value => (long)value), ids);

    private static void AssertRows(RowBatch batch, int first, int count)
    {
        Assert.Equal(count, batch.Count);
        for (int i = 0; i < count; i++)
            AssertRow(batch.GetRowSpan(i), first + i);
    }

    private static void AssertRow(ReadOnlySpan<DbValue> row, int id)
    {
        Assert.Equal(2, row.Length);
        Assert.Equal((long)id, row[0].AsInteger);
        Assert.Equal(-(long)id - 1, row[1].AsInteger);
    }

    // Mirrors a physical scan: reading a batch advances its storage cursor for
    // every row in that batch. Direct row reads continue from that cursor and
    // leave CurrentBatch unchanged. The adapter adds its own row position.
    private sealed class CursorSource(int rowCount, int batchSize, bool reuseBatch = true) :
        IOperator, IBatchOperator, IRowBufferReuseController
    {
        private int _next;
        private bool _reuseRow = true;
        private DbValue[]? _rowBuffer;
        private RowBatch? _batch;
        private TaskCompletionSource<bool>? _nextRowGate;
        private TaskCompletionSource<bool>? _nextBatchGate;
        private TaskCompletionSource<bool>? _batchStarted;

        public ColumnDefinition[] OutputSchema { get; } =
        [
            new() { Name = "id", Type = DbType.Integer },
            new() { Name = "value", Type = DbType.Integer },
        ];
        public bool ReusesCurrentRowBuffer => _reuseRow;
        public bool ReusesCurrentBatch => reuseBatch;
        public DbValue[] Current { get; private set; } = [];
        public RowBatch CurrentBatch => _batch ??= new RowBatch(2, batchSize);
        public int RowReadCalls { get; private set; }
        public TaskCompletionSource<bool> RowReadStarted { get; private set; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask OpenAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            _next = 0;
            _batch = null;
            _rowBuffer = null;
            _nextRowGate = null;
            RowReadCalls = 0;
            RowReadStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _nextBatchGate = null;
            _batchStarted = null;
            Current = [];
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            RowReadCalls++;
            if (_nextRowGate is not { } gate)
                return ValueTask.FromResult(ReadRow());

            _nextRowGate = null;
            RowReadStarted.TrySetResult(true);
            return ReadRowAfterGateAsync(gate.Task, ct);
        }

        public TaskCompletionSource<bool> SuspendNextRowRead()
        {
            _nextRowGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
            RowReadStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
            return _nextRowGate;
        }

        private async ValueTask<bool> ReadRowAfterGateAsync(Task gate, CancellationToken ct)
        {
            await gate.WaitAsync(ct);
            ct.ThrowIfCancellationRequested();
            return ReadRow();
        }

        private bool ReadRow()
        {
            if (_next >= rowCount) return false;
            DbValue[] row = _reuseRow ? (_rowBuffer ??= new DbValue[2]) : new DbValue[2];
            WriteRow(row, _next++);
            Current = row;
            return true;
        }

        public ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            if (_nextBatchGate is not { } gate)
                return ValueTask.FromResult(ReadBatch());

            _nextBatchGate = null;
            _batchStarted!.TrySetResult(true);
            return ReadAfterGateAsync(gate.Task, ct);
        }

        public Task BlockNextBatchRead()
        {
            _nextBatchGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _batchStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
            return _batchStarted.Task;
        }

        private async ValueTask<bool> ReadAfterGateAsync(Task gate, CancellationToken ct)
        {
            await gate.WaitAsync(ct);
            ct.ThrowIfCancellationRequested();
            return ReadBatch();
        }

        private bool ReadBatch()
        {
            RowBatch batch = reuseBatch ? CurrentBatch : new RowBatch(2, batchSize);
            batch.Reset();
            while (_next < rowCount && batch.Count < batch.Capacity)
            {
                int rowIndex = batch.Count;
                WriteRow(batch.GetWritableRowSpan(rowIndex), _next++);
                batch.CommitWrittenRow(rowIndex);
            }
            _batch = batch;
            return batch.Count > 0;
        }

        public void SetReuseCurrentRowBuffer(bool reuse)
        {
            _reuseRow = reuse;
            if (!reuse) _rowBuffer = null;
        }

        public ValueTask DisposeAsync()
        {
            _batch = null;
            _rowBuffer = null;
            Current = [];
            return ValueTask.CompletedTask;
        }

        private static void WriteRow(Span<DbValue> row, int id)
        {
            row[0] = DbValue.FromInteger(id);
            row[1] = DbValue.FromInteger(-(long)id - 1);
        }
    }
}
