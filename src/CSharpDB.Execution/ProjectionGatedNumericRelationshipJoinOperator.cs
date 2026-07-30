using CSharpDB.Primitives;

namespace CSharpDB.Execution;

/// <summary>
/// Defers the numeric relationship scan decision until simple projection pushdown
/// identifies a key-only output. All other shapes retain the planner's original join.
/// </summary>
internal sealed class ProjectionGatedNumericRelationshipJoinOperator :
    IOperator,
    IBatchOperator,
    IBatchBackedRowOperator,
    IProjectionPushdownTarget,
    IEstimatedRowCountProvider,
    IBatchBufferReuseController,
    IUnaryOperatorSource,
    IPhysicalOperatorChildren,
    IPhysicalOperatorMetadataProvider
{
    private const int DefaultBatchSize = 64;

    private readonly IOperator _fallback;
    private readonly Func<NumericRelationshipIndexJoinOperator> _createNumericJoin;
    private readonly int[] _numericJoinCoveredColumnIndices;
    private IOperator? _numericJoin;
    private IOperator _active;
    private IBatchOperator? _activeBatchSource;
    private bool _projectionConfigured;
    private bool _opened;
    private bool _disposed;
    private bool _reuseCurrentBatch = true;
    private RowBatch? _currentBatch;

    public ProjectionGatedNumericRelationshipJoinOperator(
        IOperator fallback,
        Func<NumericRelationshipIndexJoinOperator> createNumericJoin,
        int[] numericJoinCoveredColumnIndices)
    {
        ArgumentNullException.ThrowIfNull(fallback);
        ArgumentNullException.ThrowIfNull(createNumericJoin);
        ArgumentNullException.ThrowIfNull(numericJoinCoveredColumnIndices);

        _fallback = PhysicalPlanCapture.WrapIfActive(fallback);
        _createNumericJoin = createNumericJoin;
        _numericJoinCoveredColumnIndices = (int[])numericJoinCoveredColumnIndices.Clone();
        Array.Sort(_numericJoinCoveredColumnIndices);
        _active = _fallback;
        _activeBatchSource = BatchSourceHelper.TryGetBatchSource(_active);
        OutputSchema = _fallback.OutputSchema;
    }

    public ColumnDefinition[] OutputSchema { get; private set; }

    public bool ReusesCurrentRowBuffer => _active.ReusesCurrentRowBuffer;

    public DbValue[] Current => _active.Current;

    public int? EstimatedRowCount =>
        (_active as IEstimatedRowCountProvider)?.EstimatedRowCount ??
        (_fallback as IEstimatedRowCountProvider)?.EstimatedRowCount;

    IOperator IUnaryOperatorSource.Source => _active;
    IReadOnlyList<IOperator> IPhysicalOperatorChildren.PhysicalChildren => [_active];
    PhysicalOperatorMetadata IPhysicalOperatorMetadataProvider.GetPhysicalOperatorMetadata()
        => PhysicalPlanCapture.Unwrap(_active) is IPhysicalOperatorMetadataProvider provider
            ? provider.GetPhysicalOperatorMetadata()
            : default;

    IBatchOperator IBatchBackedRowOperator.BatchSource => this;

    bool IBatchOperator.ReusesCurrentBatch => _activeBatchSource?.ReusesCurrentBatch ?? _reuseCurrentBatch;

    RowBatch IBatchOperator.CurrentBatch =>
        _activeBatchSource?.CurrentBatch ?? EnsureBatch(OutputSchema.Length);

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(ProjectionGatedNumericRelationshipJoinOperator));
        if (_opened)
            return;

        _opened = true;
        if (_active is IBatchBufferReuseController batchController)
            batchController.SetReuseCurrentBatch(_reuseCurrentBatch);

        await _active.OpenAsync(ct);
        _activeBatchSource = BatchSourceHelper.TryGetBatchSource(_active);
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        => _active.MoveNextAsync(ct);

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        if (_activeBatchSource != null)
            return await _activeBatchSource.MoveNextBatchAsync(ct);

        RowBatch batch = _reuseCurrentBatch
            ? EnsureBatch(OutputSchema.Length)
            : CreateBatch(OutputSchema.Length);
        batch.Reset();
        while (batch.Count < batch.Capacity && await _active.MoveNextAsync(ct))
        {
            _active.Current.CopyTo(batch.GetWritableRowSpan(batch.Count));
            batch.CommitWrittenRow(batch.Count);
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    public bool TrySetOutputProjection(int[] columnIndices, ColumnDefinition[] outputSchema)
    {
        ArgumentNullException.ThrowIfNull(columnIndices);
        ArgumentNullException.ThrowIfNull(outputSchema);

        if (_opened ||
            _disposed ||
            _projectionConfigured ||
            columnIndices.Length == 0 ||
            columnIndices.Length != outputSchema.Length)
        {
            return false;
        }

        if (IsNumericJoinCoveredProjection(columnIndices))
        {
            IOperator numericJoin = _numericJoin ??=
                PhysicalPlanCapture.WrapIfActive(_createNumericJoin());
            if (numericJoin is IBatchBufferReuseController batchController)
                batchController.SetReuseCurrentBatch(_reuseCurrentBatch);

            if (PhysicalPlanCapture.Unwrap(numericJoin) is IProjectionPushdownTarget projectionTarget &&
                projectionTarget.TrySetOutputProjection(columnIndices, outputSchema))
            {
                _active = numericJoin;
                _activeBatchSource = BatchSourceHelper.TryGetBatchSource(numericJoin);
                OutputSchema = outputSchema;
                _projectionConfigured = true;
                _currentBatch = null;
                return true;
            }
        }

        // Preserve the existing plan's own projection optimization for non-covered
        // simple column lists rather than forcing a materializing projection above it.
        if (PhysicalPlanCapture.Unwrap(_fallback) is IProjectionPushdownTarget fallbackProjection &&
            fallbackProjection.TrySetOutputProjection(columnIndices, outputSchema))
        {
            OutputSchema = outputSchema;
            _projectionConfigured = true;
            _currentBatch = null;
            return true;
        }

        return false;
    }

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        if (_fallback is IBatchBufferReuseController fallbackController)
            fallbackController.SetReuseCurrentBatch(reuse);
        if (_numericJoin is IBatchBufferReuseController numericController)
            numericController.SetReuseCurrentBatch(reuse);

        _currentBatch = null;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        IOperator first = _active;
        IOperator? second = ReferenceEquals(first, _fallback) ? _numericJoin : _fallback;
        try
        {
            await first.DisposeAsync();
        }
        finally
        {
            if (second != null)
                await second.DisposeAsync();
            _activeBatchSource = null;
            _currentBatch = null;
        }
    }

    private bool IsNumericJoinCoveredProjection(ReadOnlySpan<int> columnIndices)
    {
        for (int i = 0; i < columnIndices.Length; i++)
        {
            if (Array.BinarySearch(_numericJoinCoveredColumnIndices, columnIndices[i]) < 0)
                return false;
        }

        return true;
    }

    private RowBatch EnsureBatch(int columnCount)
    {
        if (_currentBatch == null || _currentBatch.ColumnCount != columnCount)
            _currentBatch = CreateBatch(columnCount);

        return _currentBatch;
    }

    private static RowBatch CreateBatch(int columnCount) => new(columnCount, DefaultBatchSize);
}
