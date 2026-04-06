using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Client;

/// <summary>
/// Direct-transport forward-only SQL cursor for incremental row consumption.
/// </summary>
public sealed class ForwardOnlyQueryCursor : IAsyncDisposable
{
    private readonly QueryResult _result;
    private int _disposed;

    internal ForwardOnlyQueryCursor(QueryResult result)
    {
        _result = result;
        ColumnNames = result.Schema.Select(column => column.Name).ToArray();
    }

    public string[] ColumnNames { get; }

    public async ValueTask<List<object?[]>> ReadNextAsync(int maxRows, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxRows, 1);
        ThrowIfDisposed();

        var rows = new List<object?[]>(maxRows);
        while (rows.Count < maxRows && await _result.MoveNextAsync(ct))
            rows.Add(ToObjects(_result.Current));

        return rows;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
            await _result.DisposeAsync();
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0)
            throw new ObjectDisposedException(nameof(ForwardOnlyQueryCursor));
    }

    private static object?[] ToObjects(DbValue[] row)
    {
        var values = new object?[row.Length];
        for (int i = 0; i < row.Length; i++)
            values[i] = ToObject(row[i]);
        return values;
    }

    private static object? ToObject(DbValue value) => value.Type switch
    {
        DbType.Null => null,
        DbType.Integer => value.AsInteger,
        DbType.Real => value.AsReal,
        DbType.Text => value.AsText,
        DbType.Blob => value.AsBlob,
        _ => throw new CSharpDbClientException($"Unsupported DbValue type '{value.Type}'."),
    };
}
