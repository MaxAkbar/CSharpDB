using System.Buffers.Binary;
using CSharpDB.Primitives;

namespace CSharpDB.Execution;

/// <summary>
/// Cost-gated INNER JOIN operator for a declared INTEGER primary-key/foreign-key relationship.
/// It merge-scans the existing foreign-key support index with the left primary-key table,
/// derives the join value from their shared numeric key, and expands the right row IDs in
/// each payload.
/// </summary>
/// <remarks>
/// This operator is an internal implementation detail of the automatic planner optimization.
/// Conservative cardinality, source-shape, join-shape, and projection gates select it;
/// internal benchmark/test modes can select it explicitly for comparison.
/// </remarks>
internal sealed class NumericRelationshipIndexJoinOperator :
    IOperator,
    IBatchOperator,
    IBatchBackedRowOperator,
    IProjectionPushdownTarget,
    IEstimatedRowCountProvider,
    IBatchBufferReuseController
{
    private const int DefaultBatchSize = 64;

    private readonly BTree _leftTableTree;
    private readonly BTree _rightTableTree;
    private readonly IIndexStore _rightForeignKeyIndex;
    private readonly TableSchema _leftSchema;
    private readonly TableSchema _rightSchema;
    private readonly int _leftPrimaryKeyColumnIndex;
    private readonly int _rightPrimaryKeyColumnIndex;
    private readonly bool _rightPrimaryKeyIsInteger;
    private readonly int _rightForeignKeyColumnIndex;
    private readonly IndexScanRange _scanRange;
    private readonly IRecordSerializer _recordSerializer;
    private readonly int? _estimatedRowCount;

    private IIndexCursor? _rightIndexCursor;
    private BTreeCursor? _leftTableCursor;
    private bool _leftTableCursorInitialized;
    private bool _leftTableCursorHasCurrent;
    private ReadOnlyMemory<byte> _pendingRightRowIds;
    private int _pendingRightRowIdOffset;
    private long _currentJoinKey;
    private long _currentRightRowId;
    private DbValue[]? _leftRowBuffer;
    private DbValue[]? _rightRowBuffer;
    private int[]? _projectionColumnIndices;
    private int[]? _leftDecodeColumnIndices;
    private int[]? _rightDecodeColumnIndices;
    private bool _reuseCurrentBatch = true;
    private RowBatch _currentBatch;

    public NumericRelationshipIndexJoinOperator(
        BTree leftTableTree,
        BTree rightTableTree,
        IIndexStore rightForeignKeyIndex,
        TableSchema leftSchema,
        TableSchema rightSchema,
        int rightForeignKeyColumnIndex,
        IndexScanRange scanRange,
        IRecordSerializer? recordSerializer = null,
        int? estimatedRowCount = null)
    {
        ArgumentNullException.ThrowIfNull(leftTableTree);
        ArgumentNullException.ThrowIfNull(rightTableTree);
        ArgumentNullException.ThrowIfNull(rightForeignKeyIndex);
        ArgumentNullException.ThrowIfNull(leftSchema);
        ArgumentNullException.ThrowIfNull(rightSchema);

        int leftPrimaryKeyColumnIndex = leftSchema.PrimaryKeyColumnIndex;
        if (leftPrimaryKeyColumnIndex < 0 ||
            leftPrimaryKeyColumnIndex >= leftSchema.Columns.Count ||
            leftSchema.Columns[leftPrimaryKeyColumnIndex].Type != DbType.Integer)
        {
            throw new ArgumentException(
                "The relationship scan requires an INTEGER primary key on the left table.",
                nameof(leftSchema));
        }

        if (rightForeignKeyColumnIndex < 0 ||
            rightForeignKeyColumnIndex >= rightSchema.Columns.Count ||
            rightSchema.Columns[rightForeignKeyColumnIndex].Type != DbType.Integer)
        {
            throw new ArgumentOutOfRangeException(
                nameof(rightForeignKeyColumnIndex),
                "The relationship scan requires an INTEGER foreign-key column on the right table.");
        }

        _leftTableTree = leftTableTree;
        _rightTableTree = rightTableTree;
        _rightForeignKeyIndex = rightForeignKeyIndex;
        _leftSchema = leftSchema;
        _rightSchema = rightSchema;
        _leftPrimaryKeyColumnIndex = leftPrimaryKeyColumnIndex;
        _rightPrimaryKeyColumnIndex = rightSchema.PrimaryKeyColumnIndex;
        _rightPrimaryKeyIsInteger =
            _rightPrimaryKeyColumnIndex >= 0 &&
            _rightPrimaryKeyColumnIndex < rightSchema.Columns.Count &&
            rightSchema.Columns[_rightPrimaryKeyColumnIndex].Type == DbType.Integer;
        _rightForeignKeyColumnIndex = rightForeignKeyColumnIndex;
        _scanRange = scanRange;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        _estimatedRowCount = estimatedRowCount > 0 ? estimatedRowCount : null;

        OutputSchema = TableSchema.CreateJoinSchema(leftSchema, rightSchema).Columns.ToArray();
        _currentBatch = CreateBatch(OutputSchema.Length);
    }

    public ColumnDefinition[] OutputSchema { get; private set; }

    public bool ReusesCurrentRowBuffer => false;

    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public int? EstimatedRowCount => _estimatedRowCount;

    IBatchOperator IBatchBackedRowOperator.BatchSource => this;

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        await DisposeCursorsAsync();
        ct.ThrowIfCancellationRequested();

        _rightIndexCursor = _rightForeignKeyIndex.CreateCursor(_scanRange);
        // BTree.CreateCursor(range) records the table range read but deliberately leaves
        // positioning to the caller. The first relationship key lazily seeks this cursor,
        // after which both cursors advance monotonically as a merge scan.
        _leftTableCursor = _leftTableTree.CreateCursor(_scanRange);
        _leftTableCursorInitialized = false;
        _leftTableCursorHasCurrent = false;
        _pendingRightRowIds = ReadOnlyMemory<byte>.Empty;
        _pendingRightRowIdOffset = 0;
        _currentJoinKey = 0;
        _currentRightRowId = 0;
        _leftRowBuffer = null;
        _rightRowBuffer = null;
        Current = Array.Empty<DbValue>();
        _currentBatch = CreateBatch(OutputSchema.Length);
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (!await TryAdvanceAsync(ct))
        {
            Current = Array.Empty<DbValue>();
            return false;
        }

        var row = OutputSchema.Length == 0
            ? Array.Empty<DbValue>()
            : new DbValue[OutputSchema.Length];
        WriteCurrentRow(row);
        Current = row;
        return true;
    }

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        RowBatch batch = _reuseCurrentBatch
            ? EnsureBatch(OutputSchema.Length)
            : CreateBatch(OutputSchema.Length);
        batch.Reset();

        while (batch.Count < batch.Capacity && await TryAdvanceAsync(ct))
        {
            int rowIndex = batch.Count;
            WriteCurrentRow(batch.GetWritableRowSpan(rowIndex));
            batch.CommitWrittenRow(rowIndex);
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;

    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        if (!reuse)
            _currentBatch = CreateBatch(OutputSchema.Length);
    }

    public bool TrySetOutputProjection(int[] columnIndices, ColumnDefinition[] outputSchema)
    {
        ArgumentNullException.ThrowIfNull(columnIndices);
        ArgumentNullException.ThrowIfNull(outputSchema);

        if (columnIndices.Length != outputSchema.Length)
            return false;

        // Projection changes affect whether the current parent/index bucket has already
        // been decoded. The planner always pushes projection before OpenAsync; reject a
        // mid-scan change rather than risk reading stale or absent decode buffers.
        if (_rightIndexCursor != null || _leftTableCursor != null)
            return false;

        int compositeColumnCount = _leftSchema.Columns.Count + _rightSchema.Columns.Count;
        for (int i = 0; i < columnIndices.Length; i++)
        {
            if (columnIndices[i] < 0 || columnIndices[i] >= compositeColumnCount)
                return false;
        }

        _projectionColumnIndices = (int[])columnIndices.Clone();
        OutputSchema = outputSchema;
        BuildDecodeColumnMaps();
        _currentBatch = CreateBatch(OutputSchema.Length);
        return true;
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeCursorsAsync();

        _pendingRightRowIds = ReadOnlyMemory<byte>.Empty;
        _pendingRightRowIdOffset = 0;
        _leftRowBuffer = null;
        _rightRowBuffer = null;
        _currentJoinKey = 0;
        _currentRightRowId = 0;
        Current = Array.Empty<DbValue>();
    }

    private async ValueTask<bool> TryAdvanceAsync(CancellationToken ct)
    {
        while (true)
        {
            while (_pendingRightRowIdOffset + sizeof(long) <= _pendingRightRowIds.Length)
            {
                long rightRowId = BinaryPrimitives.ReadInt64LittleEndian(
                    _pendingRightRowIds.Span.Slice(_pendingRightRowIdOffset, sizeof(long)));
                _pendingRightRowIdOffset += sizeof(long);

                if (RequiresRightRowDecode())
                {
                    ReadOnlyMemory<byte>? rightPayload;
                    if (_rightTableTree.TryFindCachedMemory(rightRowId, out ReadOnlyMemory<byte>? cachedRightPayload))
                    {
                        rightPayload = cachedRightPayload;
                    }
                    else
                    {
                        rightPayload = await _rightTableTree.FindMemoryAsync(rightRowId, ct);
                    }

                    if (rightPayload is not { } rightPayloadMemory)
                        continue;

                    DecodeRightRow(rightPayloadMemory.Span);
                }

                _currentRightRowId = rightRowId;
                return true;
            }

            if (!await TryLoadNextRelationshipAsync(ct))
                return false;
        }
    }

    private async ValueTask<bool> TryLoadNextRelationshipAsync(CancellationToken ct)
    {
        if (_rightIndexCursor == null || _leftTableCursor == null)
            return false;

        while (await _rightIndexCursor.MoveNextAsync(ct))
        {
            ReadOnlyMemory<byte> rightRowIds = _rightIndexCursor.CurrentValue;
            if (rightRowIds.IsEmpty)
                continue;
            if (rightRowIds.Length % sizeof(long) != 0)
            {
                throw new InvalidOperationException(
                    "Numeric relationship index payloads must contain only 64-bit row identifiers.");
            }

            long joinKey = _rightIndexCursor.CurrentKey;
            if (!await MoveLeftCursorToAsync(joinKey, ct))
                return false;

            // The right index alone does not prove that a matching left row exists. A
            // left key greater than the current relationship key identifies an orphan;
            // keep that left row positioned for the next right-index entry.
            if (_leftTableCursor.CurrentKey != joinKey)
                continue;

            if (RequiresLeftRowDecode())
                DecodeLeftRow(_leftTableCursor.CurrentValue.Span);

            _currentJoinKey = joinKey;
            _pendingRightRowIds = rightRowIds;
            _pendingRightRowIdOffset = 0;
            return true;
        }

        return false;
    }

    private async ValueTask<bool> MoveLeftCursorToAsync(long joinKey, CancellationToken ct)
    {
        if (_leftTableCursor == null)
            return false;

        if (!_leftTableCursorInitialized)
        {
            _leftTableCursorHasCurrent = await _leftTableCursor.SeekAsync(joinKey, ct);
            _leftTableCursorInitialized = true;
        }

        while (_leftTableCursorHasCurrent && _leftTableCursor.CurrentKey < joinKey)
            _leftTableCursorHasCurrent = await _leftTableCursor.MoveNextAsync(ct);

        return _leftTableCursorHasCurrent;
    }

    private async ValueTask DisposeCursorsAsync()
    {
        IIndexCursor? rightIndexCursor = _rightIndexCursor;
        BTreeCursor? leftTableCursor = _leftTableCursor;
        _rightIndexCursor = null;
        _leftTableCursor = null;
        _leftTableCursorInitialized = false;
        _leftTableCursorHasCurrent = false;

        try
        {
            if (rightIndexCursor != null)
                await rightIndexCursor.DisposeAsync();
        }
        finally
        {
            if (leftTableCursor != null)
                await leftTableCursor.DisposeAsync();
        }
    }

    private void BuildDecodeColumnMaps()
    {
        if (_projectionColumnIndices == null)
        {
            _leftDecodeColumnIndices = null;
            _rightDecodeColumnIndices = null;
            return;
        }

        var leftColumns = new HashSet<int>();
        var rightColumns = new HashSet<int>();
        int leftColumnCount = _leftSchema.Columns.Count;

        for (int i = 0; i < _projectionColumnIndices.Length; i++)
        {
            int columnIndex = _projectionColumnIndices[i];
            if (columnIndex < leftColumnCount)
            {
                if (columnIndex != _leftPrimaryKeyColumnIndex)
                    leftColumns.Add(columnIndex);
                continue;
            }

            int rightColumnIndex = columnIndex - leftColumnCount;
            if (rightColumnIndex == _rightForeignKeyColumnIndex ||
                (_rightPrimaryKeyIsInteger && rightColumnIndex == _rightPrimaryKeyColumnIndex))
            {
                continue;
            }

            rightColumns.Add(rightColumnIndex);
        }

        _leftDecodeColumnIndices = leftColumns.Order().ToArray();
        _rightDecodeColumnIndices = rightColumns.Order().ToArray();
    }

    private bool RequiresLeftRowDecode()
        => _projectionColumnIndices == null || _leftDecodeColumnIndices is { Length: > 0 };

    private bool RequiresRightRowDecode()
        => _projectionColumnIndices == null || _rightDecodeColumnIndices is { Length: > 0 };

    private void DecodeLeftRow(ReadOnlySpan<byte> payload)
    {
        _leftRowBuffer ??= new DbValue[_leftSchema.Columns.Count];
        DecodeRow(payload, _leftRowBuffer, _leftDecodeColumnIndices);
    }

    private void DecodeRightRow(ReadOnlySpan<byte> payload)
    {
        _rightRowBuffer ??= new DbValue[_rightSchema.Columns.Count];
        DecodeRow(payload, _rightRowBuffer, _rightDecodeColumnIndices);
    }

    private void DecodeRow(ReadOnlySpan<byte> payload, DbValue[] destination, int[]? selectedColumns)
    {
        if (selectedColumns == null)
        {
            int decoded = _recordSerializer.DecodeInto(payload, destination);
            if (decoded < destination.Length)
                Array.Fill(destination, DbValue.Null, decoded, destination.Length - decoded);
            return;
        }

        Array.Fill(destination, DbValue.Null);
        if (selectedColumns.Length > 0)
            _recordSerializer.DecodeSelectedInto(payload, destination, selectedColumns);
    }

    private void WriteCurrentRow(Span<DbValue> destination)
    {
        int[]? projection = _projectionColumnIndices;
        if (projection == null)
        {
            _leftRowBuffer!.CopyTo(destination);
            _rightRowBuffer!.CopyTo(destination[_leftSchema.Columns.Count..]);
            return;
        }

        int leftColumnCount = _leftSchema.Columns.Count;
        DbValue joinKeyValue = DbValue.FromInteger(_currentJoinKey);
        DbValue rightRowIdValue = DbValue.FromInteger(_currentRightRowId);

        for (int i = 0; i < projection.Length; i++)
        {
            int columnIndex = projection[i];
            if (columnIndex < leftColumnCount)
            {
                destination[i] = columnIndex == _leftPrimaryKeyColumnIndex
                    ? joinKeyValue
                    : _leftRowBuffer![columnIndex];
                continue;
            }

            int rightColumnIndex = columnIndex - leftColumnCount;
            if (rightColumnIndex == _rightForeignKeyColumnIndex)
            {
                destination[i] = joinKeyValue;
            }
            else if (_rightPrimaryKeyIsInteger && rightColumnIndex == _rightPrimaryKeyColumnIndex)
            {
                destination[i] = rightRowIdValue;
            }
            else
            {
                destination[i] = _rightRowBuffer![rightColumnIndex];
            }
        }
    }

    private RowBatch EnsureBatch(int columnCount)
    {
        if (_currentBatch.ColumnCount != columnCount)
            _currentBatch = CreateBatch(columnCount);

        return _currentBatch;
    }

    private static RowBatch CreateBatch(int columnCount) => new(columnCount, DefaultBatchSize);
}
