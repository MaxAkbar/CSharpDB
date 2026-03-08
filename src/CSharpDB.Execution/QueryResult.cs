using CSharpDB.Core;

namespace CSharpDB.Execution;

public sealed class QueryResult : IAsyncDisposable
{
    private static readonly QueryResult ZeroRowsAffectedResult = new(0);
    private static readonly QueryResult OneRowAffectedResult = new(1);

    private readonly IOperator? _operator;
    private Func<ValueTask>? _disposeCallback;
    private bool _opened;
    private bool _disposed;

    // Sync fast path: pre-materialized single-row result (bypasses operator pipeline)
    private readonly bool _hasSyncLookupResult;
    private readonly DbValue[]? _syncRow;
    private bool _syncRowConsumed;

    public ColumnDefinition[] Schema { get; }
    public int RowsAffected { get; }
    public bool IsQuery => _operator != null || _hasSyncLookupResult;

    /// <summary>
    /// For SELECT queries.
    /// </summary>
    public QueryResult(IOperator op)
    {
        _operator = op;
        _disposeCallback = null;
        _hasSyncLookupResult = false;
        Schema = op.OutputSchema;
        RowsAffected = 0;
    }

    /// <summary>
    /// For DML/DDL statements (INSERT, UPDATE, DELETE, CREATE, DROP).
    /// </summary>
    public QueryResult(int rowsAffected)
    {
        _operator = null;
        _disposeCallback = null;
        _hasSyncLookupResult = false;
        Schema = Array.Empty<ColumnDefinition>();
        RowsAffected = rowsAffected;
    }

    /// <summary>
    /// For sync fast-path point lookups. Row is null when the key was not found (empty result).
    /// </summary>
    private QueryResult(DbValue[]? syncRow, ColumnDefinition[] schema)
    {
        _operator = null;
        _disposeCallback = null;
        _hasSyncLookupResult = true;
        _syncRow = syncRow;
        _syncRowConsumed = syncRow == null; // if no row, already consumed
        Schema = schema;
        RowsAffected = 0;
    }

    /// <summary>
    /// Create a QueryResult for a sync fast-path point lookup.
    /// Row is null when the key was not found.
    /// </summary>
    internal static QueryResult FromSyncLookup(DbValue[]? row, ColumnDefinition[] schema)
        => new(row, schema);

    internal static QueryResult FromRowsAffected(int rowsAffected)
        => rowsAffected switch
        {
            0 => ZeroRowsAffectedResult,
            1 => OneRowAffectedResult,
            _ => new QueryResult(rowsAffected),
        };

    public static QueryResult FromMaterializedRows(ColumnDefinition[] schema, List<DbValue[]> rows)
        => new QueryResult(new MaterializedRowsOperator(schema, rows));

    internal void SetDisposeCallback(Func<ValueTask> disposeCallback)
    {
        ArgumentNullException.ThrowIfNull(disposeCallback);

        if (_disposeCallback != null)
            throw new InvalidOperationException("A dispose callback is already registered for this QueryResult.");

        _disposeCallback = disposeCallback;
    }

    public async IAsyncEnumerable<DbValue[]> GetRowsAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        // Sync fast path: yield the pre-materialized row
        if (_hasSyncLookupResult)
        {
            if (!_syncRowConsumed)
            {
                _syncRowConsumed = true;
                if (_syncRow != null)
                    yield return _syncRow;
            }
            yield break;
        }

        if (_operator == null) yield break;
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
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        // Sync fast path
        if (_hasSyncLookupResult)
        {
            if (_syncRowConsumed) return false;
            _syncRowConsumed = true;
            if (_syncRow == null) return false;
            return true;
        }

        if (_operator == null) return false;
        if (!_opened)
        {
            await _operator.OpenAsync(ct);
            _opened = true;
        }
        return await _operator.MoveNextAsync(ct);
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
            if (_operator == null)
                throw new InvalidOperationException("No active query result.");
            return _operator.Current;
        }
    }

    /// <summary>
    /// Materialize all result rows into a list.
    /// </summary>
    public async ValueTask<List<DbValue[]>> ToListAsync(CancellationToken ct = default)
    {
        if (_hasSyncLookupResult)
        {
            if (_syncRowConsumed || _syncRow == null)
                return new List<DbValue[]>(0);

            _syncRowConsumed = true;
            return new List<DbValue[]>(1) { _syncRow };
        }

        if (_operator == null)
            return new List<DbValue[]>(0);

        bool cloneRows = _operator.ReusesCurrentRowBuffer;
        if (cloneRows && _operator is IRowBufferReuseController controller)
        {
            controller.SetReuseCurrentRowBuffer(false);
            cloneRows = _operator.ReusesCurrentRowBuffer;
        }

        bool openedNow = false;
        if (!_opened)
        {
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

        var list = initialCapacity > 0
            ? new List<DbValue[]>(initialCapacity)
            : new List<DbValue[]>();
        while (await _operator.MoveNextAsync(ct))
        {
            var row = _operator.Current;
            list.Add(cloneRows ? (DbValue[])row.Clone() : row);
        }

        return list;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        if (_operator != null)
            await _operator.DisposeAsync();

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
