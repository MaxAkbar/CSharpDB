using CSharpDB.Core;

namespace CSharpDB.Execution;

public sealed class QueryResult : IAsyncDisposable
{
    private readonly IOperator? _operator;
    private bool _opened;

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

        if (!_opened)
        {
            await _operator.OpenAsync(ct);
            _opened = true;
        }

        bool cloneRows = _operator.ReusesCurrentRowBuffer;
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

    public ValueTask DisposeAsync() => _operator?.DisposeAsync() ?? ValueTask.CompletedTask;
}
