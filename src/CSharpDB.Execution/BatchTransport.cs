using CSharpDB.Primitives;

namespace CSharpDB.Execution;

internal sealed class BatchToRowOperatorAdapter :
    IOperator,
    IRowBufferReuseController,
    IBatchBackedRowOperator,
    IPhysicalOperatorChildren
{
    private readonly IBatchOperator _source;
    private readonly IOperator? _physicalSource;
    private bool _reuseCurrentRowBuffer = true;
    private DbValue[]? _rowBuffer;
    private int _batchRowIndex;

    public BatchToRowOperatorAdapter(IBatchOperator source)
    {
        ArgumentNullException.ThrowIfNull(source);

        if (source is IOperator rowSource)
        {
            _physicalSource = PhysicalPlanCapture.WrapIfActive(rowSource);
            _source = (IBatchOperator)_physicalSource;
        }
        else
        {
            _source = source;
        }

        OutputSchema = _source.OutputSchema;
        _batchRowIndex = -1;
    }

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => _reuseCurrentRowBuffer;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public IBatchOperator BatchSource => _source;
    IReadOnlyList<IOperator> IPhysicalOperatorChildren.PhysicalChildren =>
        _physicalSource is null ? Array.Empty<IOperator>() : [_physicalSource];

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _rowBuffer = null;
        _batchRowIndex = -1;
        Current = Array.Empty<DbValue>();
        await _source.OpenAsync(ct);
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        while (true)
        {
            var batch = _source.CurrentBatch;
            if (_batchRowIndex + 1 < batch.Count)
            {
                _batchRowIndex++;
                Current = MaterializeRow(batch, _batchRowIndex);
                return true;
            }

            if (!await _source.MoveNextBatchAsync(ct))
            {
                Current = Array.Empty<DbValue>();
                return false;
            }

            _batchRowIndex = -1;
        }
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        _reuseCurrentRowBuffer = reuse;
        if (!reuse)
        {
            _rowBuffer = null;
            Current = Array.Empty<DbValue>();
        }
    }

    private DbValue[] MaterializeRow(RowBatch batch, int rowIndex)
    {
        int columnCount = batch.ColumnCount;
        if (_reuseCurrentRowBuffer)
        {
            _rowBuffer ??= columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];
            if (_rowBuffer.Length != columnCount)
                _rowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

            batch.CopyRowTo(rowIndex, _rowBuffer);
            return _rowBuffer;
        }

        var row = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];
        batch.CopyRowTo(rowIndex, row);
        return row;
    }
}

internal static class BatchSourceHelper
{
    public static IBatchOperator? TryGetBatchSource(IOperator source)
    {
        if (source is PhysicalProfilingOperator profiled)
        {
            if (profiled is IBatchOperator profiledBatch)
                return profiledBatch;
            if (profiled is IBatchBackedRowOperator profiledBatchBacked)
                return profiledBatchBacked.BatchSource;
            return null;
        }

        return source switch
        {
            IBatchOperator batchOperator => batchOperator,
            IBatchBackedRowOperator batchBacked => batchBacked.BatchSource,
            _ => null,
        };
    }
}

internal interface IUnaryOperatorSource
{
    IOperator Source { get; }
}
