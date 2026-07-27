using System.Collections.ObjectModel;
using System.Runtime.ExceptionServices;
using CSharpDB.Primitives;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Engine;

/// <summary>
/// Forward-only physical table reader over one verified retained database
/// snapshot. Rows are returned in strictly ascending table row-ID order.
/// </summary>
/// <remarks>
/// <see cref="Current"/> reuses one row buffer. Its contents remain valid only
/// until the next call to <see cref="MoveNextAsync"/> or until disposal.
/// Dispose this reader before starting another read through its owning
/// <see cref="RetainedDatabaseSnapshotSession"/>.
/// </remarks>
public sealed class RetainedDatabaseSnapshotTableReader : IAsyncDisposable
{
    private readonly BTreeCursor _cursor;
    private readonly IRecordSerializer _recordSerializer;
    private readonly Func<ValueTask> _releaseActiveRead;
    private readonly DbValue[] _rowBuffer;
    private readonly object _disposeGate = new();
    private readonly SemaphoreSlim _moveGate = new(1, 1);
    private readonly long? _afterRowIdExclusive;
    private Task? _disposeTask;
    private long _currentRowId;
    private long _lastRowId;
    private bool _firstMove = true;
    private bool _hasLastRowId;
    private bool _hasCurrent;
    private bool _completed;
    private int _disposed;

    internal RetainedDatabaseSnapshotTableReader(
        TableSchema schema,
        BTreeCursor cursor,
        IRecordSerializer recordSerializer,
        long? afterRowIdExclusive,
        Func<ValueTask> releaseActiveRead)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(cursor);
        ArgumentNullException.ThrowIfNull(recordSerializer);
        ArgumentNullException.ThrowIfNull(releaseActiveRead);

        TableName = schema.TableName;
        Columns = new ReadOnlyCollection<ColumnDefinition>(
            schema.Columns.Select(CopyColumn).ToArray());
        _cursor = cursor;
        _recordSerializer = recordSerializer;
        _afterRowIdExclusive = afterRowIdExclusive;
        _releaseActiveRead = releaseActiveRead;
        _rowBuffer = new DbValue[schema.Columns.Count];
    }

    /// <summary>The exact catalog table name.</summary>
    public string TableName { get; }

    /// <summary>
    /// Defensive copy of the table's columns in persisted row order.
    /// </summary>
    public IReadOnlyList<ColumnDefinition> Columns { get; }

    /// <summary>The physical row ID of <see cref="Current"/>.</summary>
    public long CurrentRowId
    {
        get
        {
            ThrowIfDisposed();
            if (!_hasCurrent)
                throw new InvalidOperationException("The table reader is not positioned on a row.");
            return _currentRowId;
        }
    }

    /// <summary>
    /// Current row values in persisted column order. The memory is reused by
    /// the next move.
    /// </summary>
    public ReadOnlyMemory<DbValue> Current
    {
        get
        {
            ThrowIfDisposed();
            if (!_hasCurrent)
                throw new InvalidOperationException("The table reader is not positioned on a row.");
            return _rowBuffer;
        }
    }

    /// <summary>
    /// Advances to the next physical row. The initial move starts at the first
    /// row ID strictly greater than the optional resume boundary.
    /// </summary>
    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        ThrowIfDisposed();
        if (!_moveGate.Wait(0))
        {
            throw new InvalidOperationException(
                "The physical table reader supports only one active move at a time.");
        }

        Exception? failure = null;
        bool moved = false;
        try
        {
            ThrowIfDisposed();
            moved = await MoveNextCoreAsync(ct).ConfigureAwait(false);
        }
        catch (Exception error)
        {
            failure = error;
        }
        finally
        {
            _moveGate.Release();
        }

        if (failure is null)
            return moved;

        try
        {
            await DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception cleanupError)
        {
            throw new AggregateException(failure, cleanupError);
        }

        ExceptionDispatchInfo.Capture(failure).Throw();
        return false;
    }

    private async ValueTask<bool> MoveNextCoreAsync(CancellationToken ct)
    {
        if (_completed)
            return false;

        ct.ThrowIfCancellationRequested();
        bool moved;
        if (_firstMove)
        {
            _firstMove = false;
            if (_afterRowIdExclusive == long.MaxValue)
            {
                moved = false;
            }
            else if (_afterRowIdExclusive is long boundary)
            {
                moved = await _cursor.SeekAsync(checked(boundary + 1), ct)
                    .ConfigureAwait(false);
            }
            else
            {
                moved = await _cursor.MoveNextAsync(ct).ConfigureAwait(false);
            }
        }
        else
        {
            moved = await _cursor.MoveNextAsync(ct).ConfigureAwait(false);
        }

        if (!moved)
        {
            _hasCurrent = false;
            _completed = true;
            return false;
        }

        long rowId = _cursor.CurrentKey;
        if (_afterRowIdExclusive is long exclusive && rowId <= exclusive)
        {
            throw new InvalidDataException(
                "The physical table cursor did not honor its exclusive row-ID boundary.");
        }
        if (_hasLastRowId && rowId <= _lastRowId)
        {
            throw new InvalidDataException(
                "The physical table cursor returned row IDs out of ascending order.");
        }

        ReadOnlySpan<byte> payload = _cursor.CurrentValue.Span;
        int encodedCount = _recordSerializer.GetDecodedColumnCount(payload);
        if ((uint)encodedCount > (uint)_rowBuffer.Length)
        {
            throw new InvalidDataException(
                "The physical table row contains more values than its retained schema.");
        }
        int decodedCount = _recordSerializer.DecodeInto(payload, _rowBuffer);
        if (decodedCount != encodedCount)
        {
            throw new InvalidDataException(
                "The physical table row serializer returned an inconsistent value count.");
        }
        if (decodedCount < _rowBuffer.Length)
        {
            Array.Fill(
                _rowBuffer,
                DbValue.Null,
                decodedCount,
                _rowBuffer.Length - decodedCount);
        }

        _currentRowId = rowId;
        _lastRowId = rowId;
        _hasLastRowId = true;
        _hasCurrent = true;
        return true;
    }

    public ValueTask DisposeAsync()
    {
        lock (_disposeGate)
            return new ValueTask(_disposeTask ??= DisposeCoreAsync());
    }

    private async Task DisposeCoreAsync()
    {
        Volatile.Write(ref _disposed, 1);
        await _moveGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            _hasCurrent = false;
            _completed = true;
            Array.Clear(_rowBuffer);
            try
            {
                await _cursor.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                await _releaseActiveRead().ConfigureAwait(false);
            }
            GC.SuppressFinalize(this);
        }
        finally
        {
            _moveGate.Release();
        }
    }

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    private static ColumnDefinition CopyColumn(ColumnDefinition column) => new()
    {
        Name = column.Name,
        Type = column.Type,
        Nullable = column.Nullable,
        IsPrimaryKey = column.IsPrimaryKey,
        IsIdentity = column.IsIdentity,
        IsRowVersion = column.IsRowVersion,
        Collation = column.Collation,
        DefaultSql = column.DefaultSql,
    };
}
