using System.Runtime.CompilerServices;
using CSharpDB.Primitives;

namespace CSharpDB.Execution;

public sealed class QueryResult : IAsyncDisposable
{
    private static readonly QueryResult ZeroRowsAffectedResult = new(0);
    private static readonly QueryResult OneRowAffectedResult = new(1);
    private static readonly ConditionalWeakTable<QueryResult, GeneratedIntegerKeyMetadata> s_generatedIntegerKeys = new();

    private readonly IOperator? _operator;
    private readonly IBatchOperator? _batchOperator;
    private Func<ValueTask>? _disposeCallback;
    private Func<IDisposable>? _executionScopeFactory;
    private bool _opened;
    private bool _disposed;
    private DbValue[]? _batchCurrentRow;
    private int _batchRowIndex;
    private bool _batchExhausted;

    // Sync fast path: pre-materialized single-row result (bypasses operator pipeline)
    private readonly bool _hasSyncLookupResult;
    private readonly bool _hasSyncScalarResult;
    private readonly DbValue[]? _syncRow;
    private readonly DbValue _syncScalar;
    private DbValue[]? _syncScalarRow;
    private bool _syncRowConsumed;

    public ColumnDefinition[] Schema { get; }
    public int RowsAffected { get; }
    public bool IsQuery => _operator != null || _batchOperator != null || _hasSyncLookupResult || _hasSyncScalarResult;

    /// <summary>
    /// For SELECT queries.
    /// </summary>
    public QueryResult(IOperator op)
    {
        _operator = op;
        _batchOperator = null;
        _disposeCallback = null;
        _batchRowIndex = -1;
        _hasSyncLookupResult = false;
        _hasSyncScalarResult = false;
        Schema = op.OutputSchema;
        RowsAffected = 0;
    }

    private QueryResult(IBatchOperator op)
    {
        _operator = null;
        _batchOperator = op;
        _disposeCallback = null;
        _batchRowIndex = -1;
        _hasSyncLookupResult = false;
        _hasSyncScalarResult = false;
        Schema = op.OutputSchema;
        RowsAffected = 0;
    }

    /// <summary>
    /// For DML/DDL statements (INSERT, UPDATE, DELETE, CREATE, DROP).
    /// </summary>
    public QueryResult(int rowsAffected)
    {
        _operator = null;
        _batchOperator = null;
        _disposeCallback = null;
        _batchRowIndex = -1;
        _hasSyncLookupResult = false;
        _hasSyncScalarResult = false;
        Schema = Array.Empty<ColumnDefinition>();
        RowsAffected = rowsAffected;
    }

    /// <summary>
    /// For sync fast-path point lookups. Row is null when the key was not found (empty result).
    /// </summary>
    private QueryResult(DbValue[]? syncRow, ColumnDefinition[] schema)
    {
        _operator = null;
        _batchOperator = null;
        _disposeCallback = null;
        _batchRowIndex = -1;
        _hasSyncLookupResult = true;
        _hasSyncScalarResult = false;
        _syncRow = syncRow;
        _syncRowConsumed = syncRow == null; // if no row, already consumed
        Schema = schema;
        RowsAffected = 0;
    }

    /// <summary>
    /// For sync fast-path scalar results that can defer row materialization until Current/ToList is requested.
    /// </summary>
    private QueryResult(DbValue syncScalar, ColumnDefinition[] schema)
    {
        _operator = null;
        _batchOperator = null;
        _disposeCallback = null;
        _batchRowIndex = -1;
        _hasSyncLookupResult = false;
        _hasSyncScalarResult = true;
        _syncScalar = syncScalar;
        _syncRowConsumed = false;
        Schema = schema;
        RowsAffected = 0;
    }

    /// <summary>
    /// Create a QueryResult for a sync fast-path point lookup.
    /// Row is null when the key was not found.
    /// </summary>
    internal static QueryResult FromSyncLookup(DbValue[]? row, ColumnDefinition[] schema)
        => new(row, schema);

    /// <summary>
    /// Create a QueryResult for a sync fast-path scalar result.
    /// The row wrapper is allocated lazily only if the caller inspects Current or materializes rows.
    /// </summary>
    internal static QueryResult FromSyncScalar(DbValue value, ColumnDefinition[] schema)
        => new(value, schema);

    internal static QueryResult FromBatchOperator(IBatchOperator op)
        => new(op);

    internal static QueryResult FromRowsAffected(int rowsAffected)
        => rowsAffected switch
        {
            0 => ZeroRowsAffectedResult,
            1 => OneRowAffectedResult,
            _ => new QueryResult(rowsAffected),
        };

    internal static QueryResult FromRowsAffected(int rowsAffected, long? generatedIntegerKey)
    {
        if (generatedIntegerKey.HasValue)
        {
            var result = new QueryResult(rowsAffected);
            s_generatedIntegerKeys.Add(result, new GeneratedIntegerKeyMetadata(generatedIntegerKey.Value));
            return result;
        }

        return FromRowsAffected(rowsAffected);
    }

    internal bool TryGetGeneratedIntegerKey(out long generatedIntegerKey)
    {
        if (s_generatedIntegerKeys.TryGetValue(this, out GeneratedIntegerKeyMetadata? metadata))
        {
            generatedIntegerKey = metadata.Value;
            return true;
        }

        generatedIntegerKey = default;
        return false;
    }

    private sealed class GeneratedIntegerKeyMetadata(long value)
    {
        internal long Value { get; } = value;
    }

    public static QueryResult FromMaterializedRows(ColumnDefinition[] schema, List<DbValue[]> rows)
        => new QueryResult(new MaterializedRowsOperator(schema, rows));

    internal void SetDisposeCallback(Func<ValueTask> disposeCallback)
    {
        ArgumentNullException.ThrowIfNull(disposeCallback);

        if (_disposeCallback != null)
            throw new InvalidOperationException("A dispose callback is already registered for this QueryResult.");

        _disposeCallback = disposeCallback;
    }

    internal void SetExecutionScopeFactory(Func<IDisposable> executionScopeFactory)
    {
        ArgumentNullException.ThrowIfNull(executionScopeFactory);

        if (_executionScopeFactory != null)
            throw new InvalidOperationException("An execution scope factory is already registered for this QueryResult.");

        _executionScopeFactory = executionScopeFactory;
    }

    public async IAsyncEnumerable<DbValue[]> GetRowsAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        // Sync fast path: yield the pre-materialized row
        if (_hasSyncLookupResult || _hasSyncScalarResult)
        {
            if (!_syncRowConsumed)
            {
                _syncRowConsumed = true;
                if (_hasSyncLookupResult)
                {
                    if (_syncRow != null)
                        yield return _syncRow;
                }
                else
                {
                    yield return GetOrCreateSyncScalarRow();
                }
            }
            yield break;
        }

        if (_operator != null)
        {
            bool cloneRows = _operator.ReusesCurrentRowBuffer;
            if (cloneRows && _operator is IRowBufferReuseController controller)
            {
                controller.SetReuseCurrentRowBuffer(false);
                cloneRows = _operator.ReusesCurrentRowBuffer;
            }

            while (await MoveNextAsync(ct))
            {
                var row = Current;
                yield return cloneRows ? (DbValue[])row.Clone() : row;
            }

            yield break;
        }

        if (_batchOperator == null)
            yield break;

        while (await MoveNextAsync(ct))
            yield return (DbValue[])Current.Clone();
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        // Sync fast path
        if (_hasSyncLookupResult || _hasSyncScalarResult)
        {
            if (_syncRowConsumed) return ValueTask.FromResult(false);
            _syncRowConsumed = true;
            if (_hasSyncLookupResult && _syncRow == null) return ValueTask.FromResult(false);
            return ValueTask.FromResult(true);
        }

        if (_operator != null)
            return MoveNextOperatorAsync(ct);

        if (_batchOperator == null || _batchExhausted)
            return ValueTask.FromResult(false);

        return MoveNextBatchAsync(ct);
    }

    public DbValue[] Current
    {
        get
        {
            if (_hasSyncLookupResult)
            {
                if (_syncRow == null)
                    throw new InvalidOperationException("No active query row.");

                return _syncRow;
            }

            if (_hasSyncScalarResult)
                return GetOrCreateSyncScalarRow();

            if (_operator != null)
                return _operator.Current;

            if (_batchCurrentRow != null)
                return _batchCurrentRow;

            throw new InvalidOperationException("No active query result.");
        }
    }

    /// <summary>
    /// Materialize all result rows into a list.
    /// </summary>
    public async ValueTask<List<DbValue[]>> ToListAsync(CancellationToken ct = default)
    {
        if (_hasSyncLookupResult || _hasSyncScalarResult)
        {
            if (_syncRowConsumed)
                return new List<DbValue[]>(0);

            _syncRowConsumed = true;

            if (_hasSyncLookupResult)
            {
                if (_syncRow == null)
                    return new List<DbValue[]>(0);

                return new List<DbValue[]>(1) { _syncRow };
            }

            return new List<DbValue[]>(1) { GetOrCreateSyncScalarRow() };
        }

        if (_operator != null)
        {
            bool cloneRows = _operator.ReusesCurrentRowBuffer;
            if (cloneRows && _operator is IRowBufferReuseController controller)
            {
                controller.SetReuseCurrentRowBuffer(false);
                cloneRows = _operator.ReusesCurrentRowBuffer;
            }

            bool openedNow = false;
            if (!_opened)
            {
                using IDisposable? scope = EnterExecutionScope();
                await _operator.OpenAsync(ct);
                _opened = true;
                openedNow = true;
            }

            if (openedNow &&
                !cloneRows &&
                _operator is IMaterializedRowsProvider materialized &&
                materialized.TryTakeMaterializedRows(out var materializedRows))
            {
                return materializedRows;
            }

            int initialCapacity = 0;
            if (_operator is IEstimatedRowCountProvider estimated &&
                estimated.EstimatedRowCount is int rowCount &&
                rowCount > 0)
            {
                initialCapacity = rowCount;
            }

            if (openedNow &&
                _operator is IBatchBackedRowOperator batchBacked)
            {
                return await MaterializeBatchRowsAsync(batchBacked.BatchSource, initialCapacity, -1, _executionScopeFactory, ct);
            }

            var list = initialCapacity > 0
                ? new List<DbValue[]>(initialCapacity)
                : new List<DbValue[]>();
            while (true)
            {
                bool hasRow;
                using (IDisposable? scope = EnterExecutionScope())
                {
                    hasRow = await _operator.MoveNextAsync(ct);
                }

                if (!hasRow)
                    break;

                var row = _operator.Current;
                list.Add(cloneRows ? (DbValue[])row.Clone() : row);
            }

            return list;
        }

        if (_batchOperator == null)
            return new List<DbValue[]>(0);

        if (!_opened)
        {
            using IDisposable? scope = EnterExecutionScope();
            await _batchOperator.OpenAsync(ct);
            _opened = true;
            _batchRowIndex = -1;
            _batchCurrentRow = null;
            _batchExhausted = false;
        }

        if (_batchExhausted)
            return new List<DbValue[]>(0);

        if (_batchOperator is IMaterializedRowsProvider batchMaterialized &&
            _batchRowIndex < 0 &&
            _batchOperator.CurrentBatch.Count == 0 &&
            batchMaterialized.TryTakeMaterializedRows(out var directRows))
        {
            _batchExhausted = true;
            return directRows;
        }

        int batchInitialCapacity = 0;
        if (_batchOperator is IEstimatedRowCountProvider batchEstimated &&
            batchEstimated.EstimatedRowCount is int batchRowCount &&
            batchRowCount > 0)
        {
            batchInitialCapacity = batchRowCount;
        }

        var rows = await MaterializeBatchRowsAsync(_batchOperator, batchInitialCapacity, _batchRowIndex, _executionScopeFactory, ct);
        _batchCurrentRow = null;
        _batchExhausted = true;
        return rows;
    }

    private DbValue[] GetOrCreateSyncScalarRow()
        => _syncScalarRow ??= [_syncScalar];

    private DbValue[] MaterializeBatchRow(RowBatch batch, int rowIndex)
    {
        int columnCount = batch.ColumnCount;
        _batchCurrentRow ??= columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];
        if (_batchCurrentRow.Length != columnCount)
            _batchCurrentRow = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        batch.CopyRowTo(rowIndex, _batchCurrentRow);
        return _batchCurrentRow;
    }

    private static async ValueTask<List<DbValue[]>> MaterializeBatchRowsAsync(
        IBatchOperator batchSource,
        int initialCapacity,
        int currentBatchRowIndex,
        Func<IDisposable>? executionScopeFactory,
        CancellationToken ct = default)
    {
        var list = initialCapacity > 0
            ? new List<DbValue[]>(initialCapacity)
            : new List<DbValue[]>();

        RowBatch batch = batchSource.CurrentBatch;
        int startRowIndex = Math.Max(0, currentBatchRowIndex + 1);
        for (int rowIndex = startRowIndex; rowIndex < batch.Count; rowIndex++)
        {
            var row = batch.ColumnCount == 0 ? Array.Empty<DbValue>() : new DbValue[batch.ColumnCount];
            batch.CopyRowTo(rowIndex, row);
            list.Add(row);
        }

        while (true)
        {
            bool hasNextBatch;
            using (IDisposable? scope = executionScopeFactory?.Invoke())
            {
                hasNextBatch = await batchSource.MoveNextBatchAsync(ct);
            }

            if (!hasNextBatch)
                break;

            batch = batchSource.CurrentBatch;
            int columnCount = batch.ColumnCount;
            for (int rowIndex = 0; rowIndex < batch.Count; rowIndex++)
            {
                var row = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];
                batch.CopyRowTo(rowIndex, row);
                list.Add(row);
            }
        }

        return list;
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed)
            return ValueTask.CompletedTask;

        _disposed = true;

        if (_operator != null)
            return DisposeOperatorAsync();

        if (_batchOperator != null)
            return DisposeBatchOperatorAsync();

        if (_disposeCallback != null)
            return _disposeCallback();

        return ValueTask.CompletedTask;
    }

    private IDisposable? EnterExecutionScope() => _executionScopeFactory?.Invoke();

    private async ValueTask<bool> MoveNextOperatorAsync(CancellationToken ct)
    {
        if (_operator == null)
            return false;

        if (!_opened)
        {
            using IDisposable? openScope = EnterExecutionScope();
            await _operator.OpenAsync(ct);
            _opened = true;
        }

        using IDisposable? moveScope = EnterExecutionScope();
        return await _operator.MoveNextAsync(ct);
    }

    private async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct)
    {
        if (_batchOperator == null || _batchExhausted)
            return false;

        if (!_opened)
        {
            using IDisposable? scope = EnterExecutionScope();
            await _batchOperator.OpenAsync(ct);
            _opened = true;
            _batchRowIndex = -1;
            _batchCurrentRow = null;
            _batchExhausted = false;
        }

        while (true)
        {
            RowBatch batch = _batchOperator.CurrentBatch;
            if (_batchRowIndex + 1 < batch.Count)
            {
                _batchRowIndex++;
                _batchCurrentRow = MaterializeBatchRow(batch, _batchRowIndex);
                return true;
            }

            using IDisposable? scope = EnterExecutionScope();
            if (!await _batchOperator.MoveNextBatchAsync(ct))
            {
                _batchCurrentRow = null;
                _batchExhausted = true;
                return false;
            }

            _batchRowIndex = -1;
        }
    }

    private async ValueTask DisposeOperatorAsync()
    {
        if (_operator != null)
        {
            using IDisposable? scope = EnterExecutionScope();
            await _operator.DisposeAsync();
        }

        if (_disposeCallback != null)
            await _disposeCallback();
    }

    private async ValueTask DisposeBatchOperatorAsync()
    {
        if (_batchOperator != null)
        {
            using IDisposable? scope = EnterExecutionScope();
            await _batchOperator.DisposeAsync();
        }

        if (_disposeCallback != null)
            await _disposeCallback();
    }

    private sealed class MaterializedRowsOperator : IOperator, IMaterializedRowsProvider, IEstimatedRowCountProvider
    {
        private List<DbValue[]>? _rows;
        private int _index = -1;

        internal MaterializedRowsOperator(ColumnDefinition[] outputSchema, List<DbValue[]> rows)
        {
            OutputSchema = outputSchema;
            _rows = rows;
        }

        public ColumnDefinition[] OutputSchema { get; }
        public bool ReusesCurrentRowBuffer => false;
        public int? EstimatedRowCount => _rows?.Count ?? 0;

        public DbValue[] Current => _rows is not null && _index >= 0 && _index < _rows.Count
            ? _rows[_index]
            : throw new InvalidOperationException("No active query row.");

        public ValueTask OpenAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();

            if (_rows is null)
                return ValueTask.FromResult(false);

            int nextIndex = _index + 1;
            if (nextIndex >= _rows.Count)
                return ValueTask.FromResult(false);

            _index = nextIndex;
            return ValueTask.FromResult(true);
        }

        public bool TryTakeMaterializedRows(out List<DbValue[]> rows)
        {
            rows = _rows ?? new List<DbValue[]>();
            _rows = null;
            return true;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
