using CSharpDB.Primitives;

namespace CSharpDB.Execution;

/// <summary>
/// Reads a bounded physical integer primary-key range, projecting directly from each table record.
/// The tree and cursor belong to one execution; callers must resolve the tree from the current pager.
/// </summary>
internal sealed class PrimaryKeyRangeScanOperator :
    IOperator,
    IBatchOperator,
    IRowBufferReuseController,
    IBatchBufferReuseController,
    IPhysicalOperatorMetadataProvider
{
    private const int DefaultBatchSize = 64;

    private readonly BTree _tree;
    private readonly TableSchema _schema;
    private readonly IndexScanRange _range;
    private readonly IRecordSerializer _serializer;
    private readonly int[]? _projection;
    private readonly bool _primaryKeyOnly;
    private readonly int _decodedColumnCount;
    private BTreeCursor? _cursor;
    private bool _started;
    private bool _exhausted;
    private bool _reuseCurrentRowBuffer = true;
    private bool _reuseCurrentBatch = true;
    private DbValue[]? _rowBuffer;
    private DbValue[]? _decodedRow;
    private RowBatch? _currentBatch;

    public PrimaryKeyRangeScanOperator(
        BTree tree,
        TableSchema schema,
        IndexScanRange range,
        IRecordSerializer serializer,
        int[]? projection,
        ColumnDefinition[] outputColumns)
    {
        _tree = tree;
        _schema = schema;
        _range = range;
        _serializer = serializer;
        _projection = projection;
        OutputSchema = outputColumns;

        if (projection is null)
        {
            if (outputColumns.Length != schema.Columns.Count)
                throw new ArgumentException("Star output must match the table schema.", nameof(outputColumns));
            _decodedColumnCount = schema.Columns.Count;
            return;
        }

        if (outputColumns.Length != projection.Length)
            throw new ArgumentException("Projection and output column counts must match.", nameof(outputColumns));
        int primaryKey = schema.PrimaryKeyColumnIndex;
        _primaryKeyOnly = primaryKey >= 0;
        int maxColumn = -1;
        foreach (int column in projection)
        {
            if ((uint)column >= (uint)schema.Columns.Count)
                throw new ArgumentOutOfRangeException(nameof(projection));
            _primaryKeyOnly &= column == primaryKey;
            maxColumn = Math.Max(maxColumn, column);
        }
        _decodedColumnCount = maxColumn + 1;
    }

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => _reuseCurrentRowBuffer;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public bool ReusesCurrentBatch => _reuseCurrentBatch;
    public RowBatch CurrentBatch => _currentBatch ??= new RowBatch(OutputSchema.Length, 0);

    PhysicalOperatorMetadata IPhysicalOperatorMetadataProvider.GetPhysicalOperatorMetadata()
        => new(AccessPath: PhysicalAccessPath.OrderedIndex, ObjectName: _schema.TableName);

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        if (_cursor is not null)
            await _cursor.DisposeAsync();

        // This overload records the table-row read range; it does not enforce cursor bounds.
        _cursor = _tree.CreateCursor(_range);
        _started = false;
        _exhausted = _range.LowerBound.HasValue && _range.UpperBound.HasValue &&
            (_range.LowerBound > _range.UpperBound || _range.LowerBound == _range.UpperBound &&
                (!_range.LowerInclusive || !_range.UpperInclusive));
        _rowBuffer = null;
        _decodedRow = null;
        _currentBatch = null;
        Current = Array.Empty<DbValue>();
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (!await MoveToNextInRangeAsync(ct))
        {
            Current = Array.Empty<DbValue>();
            return false;
        }

        DbValue[] row = _reuseCurrentRowBuffer
            ? _rowBuffer ??= new DbValue[OutputSchema.Length]
            : new DbValue[OutputSchema.Length];
        ProjectCurrent(row);
        Current = row;
        return true;
    }

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        if (!await MoveToNextInRangeAsync(ct))
        {
            _currentBatch = null;
            return false;
        }

        // Allocate a full batch only after a caller requests one and a matching row exists.
        RowBatch batch = _reuseCurrentBatch && _currentBatch is { Capacity: DefaultBatchSize }
            ? _currentBatch
            : new RowBatch(OutputSchema.Length, DefaultBatchSize);
        batch.Reset();
        do
        {
            int rowIndex = batch.Count;
            ProjectCurrent(batch.GetWritableRowSpan(rowIndex));
            batch.CommitWrittenRow(rowIndex);
        }
        while (batch.Count < batch.Capacity && await MoveToNextInRangeAsync(ct));

        _currentBatch = batch;
        return true;
    }

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        if (_reuseCurrentRowBuffer == reuse) return;
        _reuseCurrentRowBuffer = reuse;
        _rowBuffer = null;
    }

    public void SetReuseCurrentBatch(bool reuse)
    {
        if (_reuseCurrentBatch == reuse) return;
        _reuseCurrentBatch = reuse;
        // A batch handed out with reuse disabled remains owned by its consumer.
        _currentBatch = null;
    }

    public async ValueTask DisposeAsync()
    {
        if (_cursor is not null)
        {
            await _cursor.DisposeAsync();
            _cursor = null;
        }
        _exhausted = true;
        _rowBuffer = null;
        _decodedRow = null;
        _currentBatch = null;
        Current = Array.Empty<DbValue>();
    }

    private async ValueTask<bool> MoveToNextInRangeAsync(CancellationToken ct)
    {
        if (_cursor is null || _exhausted) return false;

        bool found;
        if (!_started && _range.LowerBound.HasValue)
        {
            _started = true;
            found = await _cursor.SeekAsync(_range.LowerBound.Value, ct);
            if (found && !_range.LowerInclusive && _cursor.CurrentKey == _range.LowerBound.Value)
                found = await _cursor.MoveNextAsync(ct);
        }
        else
        {
            _started = true;
            found = await _cursor.MoveNextAsync(ct);
        }

        if (!found || _range.UpperBound.HasValue &&
            (_range.UpperInclusive ? _cursor.CurrentKey > _range.UpperBound.Value : _cursor.CurrentKey >= _range.UpperBound.Value))
        {
            _exhausted = true;
            return false;
        }
        return true;
    }

    private void ProjectCurrent(Span<DbValue> destination)
    {
        if (_primaryKeyOnly)
        {
            destination.Fill(DbValue.FromInteger(_cursor!.CurrentKey));
            return;
        }

        ReadOnlySpan<byte> payload = _cursor!.CurrentValue.Span;
        if (_projection is null)
        {
            int decoded = _serializer.DecodeInto(payload, destination);
            if (decoded < destination.Length) destination[decoded..].Fill(DbValue.Null);
            return;
        }

        // Decode only through the highest requested ordinal, padding rows stored before ALTER ADD.
        _decodedRow ??= new DbValue[_decodedColumnCount];
        int count = _serializer.DecodeInto(payload, _decodedRow);
        if (count < _decodedRow.Length) _decodedRow.AsSpan(count).Fill(DbValue.Null);
        for (int i = 0; i < _projection.Length; i++)
            destination[i] = _decodedRow[_projection[i]];
    }
}
