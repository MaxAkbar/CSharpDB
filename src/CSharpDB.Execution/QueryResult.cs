using CSharpDB.Core;

namespace CSharpDB.Execution;

public sealed class QueryResult : IAsyncDisposable
{
    private readonly IOperator? _operator;
    private bool _opened;

    // Sync fast path: pre-materialized single-row result (bypasses operator pipeline)
    private readonly DbValue[]? _syncRow;
    private bool _syncRowConsumed;

    public ColumnDefinition[] Schema { get; }
    public int RowsAffected { get; }
    public bool IsQuery => _operator != null || _syncRow != null;

    /// <summary>
    /// For SELECT queries.
    /// </summary>
    public QueryResult(IOperator op)
    {
        _operator = op;
        Schema = op.OutputSchema;
        RowsAffected = 0;
    }

    /// <summary>
    /// For DML/DDL statements (INSERT, UPDATE, DELETE, CREATE, DROP).
    /// </summary>
    public QueryResult(int rowsAffected)
    {
        _operator = null;
        Schema = Array.Empty<ColumnDefinition>();
        RowsAffected = rowsAffected;
    }

    /// <summary>
    /// For sync fast-path point lookups. Row is null when the key was not found (empty result).
    /// </summary>
    private QueryResult(DbValue[]? syncRow, ColumnDefinition[] schema)
    {
        _operator = null;
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
        if (_syncRow != null)
        {
            if (!_syncRowConsumed)
            {
                _syncRowConsumed = true;
                yield return _syncRow;
            }
            yield break;
        }

        if (_operator == null) yield break;
        EnsureMaterializedRowOwnership();
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
        if (_syncRow != null)
        {
            if (_syncRowConsumed) return false;
            _syncRowConsumed = true;
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
            if (_syncRow != null) return _syncRow;
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
        var list = new List<DbValue[]>();
        await foreach (var row in GetRowsAsync(ct))
            list.Add(row);
        return list;
    }

    private void EnsureMaterializedRowOwnership()
    {
        if (_opened || _operator is not IRowBufferReuseController controller)
            return;

        controller.SetReuseCurrentRowBuffer(false);
    }

    public ValueTask DisposeAsync() => _operator?.DisposeAsync() ?? ValueTask.CompletedTask;
}
