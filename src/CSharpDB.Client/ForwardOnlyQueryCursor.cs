using CSharpDB.Execution;
using CSharpDB.Observability;
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
    private readonly CSharpDbDeferredDiagnosticBoundary? _deferredBoundary;
    private int _released;
    private int _disposed;

    internal ForwardOnlyQueryCursor(
        QueryResult result,
        Func<ValueTask>? onDispose = null,
        CSharpDbDeferredDiagnosticBoundary? deferredBoundary = null)
    {
        _result = result;
        _onDispose = onDispose;
        _deferredBoundary = deferredBoundary;
        ColumnNames = result.Schema.Select(column => column.Name).ToArray();
    }

    public string[] ColumnNames { get; }

    public async ValueTask<List<object?[]>> ReadNextAsync(int maxRows, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxRows, 1);
        ThrowIfDisposed();

        var rows = new List<object?[]>(maxRows);
        bool terminal = false;
        IDisposable? boundaryScope = Volatile.Read(ref _released) == 0
            ? _deferredBoundary?.Enter()
            : null;
        try
        {
            while (rows.Count < maxRows)
            {
                if (!await _result.MoveNextAsync(ct))
                {
                    terminal = true;
                    break;
                }

                rows.Add(ToObjects(_result.Schema, _result.Current));
            }

            return rows;
        }
        catch
        {
            terminal = true;
            throw;
        }
        finally
        {
            if (terminal)
            {
                try
                {
                    await ReleaseAsync();
                }
                finally
                {
                    boundaryScope?.Dispose();
                    _deferredBoundary?.Dispose();
                }
            }
            else
            {
                boundaryScope?.Dispose();
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        IDisposable? boundaryScope = Volatile.Read(ref _released) == 0
            ? _deferredBoundary?.Enter()
            : null;
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
            await ReleaseAsync();
        }
        catch (Exception releaseException) when (resultDisposeException is not null)
        {
            throw new AggregateException(resultDisposeException, releaseException);
        }
        finally
        {
            boundaryScope?.Dispose();
            _deferredBoundary?.Dispose();
        }

        if (resultDisposeException is not null)
            ExceptionDispatchInfo.Capture(resultDisposeException).Throw();
    }

    private async ValueTask ReleaseAsync()
    {
        if (Interlocked.Exchange(ref _released, 1) != 0)
            return;

        if (_onDispose is not null)
            await _onDispose();
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0)
            throw new ObjectDisposedException(nameof(ForwardOnlyQueryCursor));
    }

    private static object?[] ToObjects(ColumnDefinition[] schema, DbValue[] row)
    {
        var values = new object?[row.Length];
        for (int i = 0; i < row.Length; i++)
            values[i] = ToObject(row[i], i < schema.Length ? schema[i] : null);
        return values;
    }

    private static object? ToObject(
        DbValue value,
        ColumnDefinition? column = null) => value.Type switch
    {
        DbType.Null => null,
        DbType.Integer when column?.DeclaredType?.Kind == SqlTypeKind.Boolean =>
            value.AsInteger != 0,
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
