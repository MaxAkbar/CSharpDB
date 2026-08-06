using CSharpDB.Execution;
using CSharpDB.Primitives;
using System.Runtime.ExceptionServices;
using SqlBitString = CSharpDB.Client.Models.SqlBitString;

namespace CSharpDB.Client;

/// <summary>
/// Direct-transport forward-only SQL cursor for incremental row consumption.
/// </summary>
public sealed class ForwardOnlyQueryCursor : IAsyncDisposable
{
    private readonly QueryResult _result;
    private readonly Func<ValueTask>? _onDispose;
    private int _disposed;

    internal ForwardOnlyQueryCursor(QueryResult result, Func<ValueTask>? onDispose = null)
    {
        _result = result;
        _onDispose = onDispose;
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
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        Exception? resultDisposeException = null;
        try
        {
            await _result.DisposeAsync();
        }
        catch (Exception ex)
        {
            resultDisposeException = ex;
        }

        try
        {
            if (_onDispose is not null)
                await _onDispose();
        }
        catch (Exception releaseException) when (resultDisposeException is not null)
        {
            throw new AggregateException(resultDisposeException, releaseException);
        }

        if (resultDisposeException is not null)
            ExceptionDispatchInfo.Capture(resultDisposeException).Throw();
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
        DbType.Decimal => value.AsDecimal,
        DbType.Text => value.AsText,
        DbType.Blob when value.IsBitString =>
            new SqlBitString(value.AsBlob, value.BitLength),
        DbType.Blob => value.AsBlob,
        _ => throw new CSharpDbClientException($"Unsupported DbValue type '{value.Type}'."),
    };
}
