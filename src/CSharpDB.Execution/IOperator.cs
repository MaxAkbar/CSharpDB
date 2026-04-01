using System.Collections.Generic;
using CSharpDB.Primitives;

namespace CSharpDB.Execution;

public interface IOperator : IAsyncDisposable
{
    ColumnDefinition[] OutputSchema { get; }

    /// <summary>
    /// True when the operator may reuse/mutate the same Current row buffer across MoveNext calls.
    /// Consumers that need row ownership (e.g. sort materialization) should clone when this is true.
    /// </summary>
    bool ReusesCurrentRowBuffer => true;

    ValueTask OpenAsync(CancellationToken ct = default);
    ValueTask<bool> MoveNextAsync(CancellationToken ct = default);
    DbValue[] Current { get; }
}

internal interface IBatchOperator : IAsyncDisposable
{
    ColumnDefinition[] OutputSchema { get; }
    bool ReusesCurrentBatch => true;
    ValueTask OpenAsync(CancellationToken ct = default);
    ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default);
    RowBatch CurrentBatch { get; }
}

internal interface IRowBufferReuseController
{
    void SetReuseCurrentRowBuffer(bool reuse);
}

internal interface IBatchBufferReuseController
{
    void SetReuseCurrentBatch(bool reuse);
}

internal interface IBatchBackedRowOperator
{
    IBatchOperator BatchSource { get; }
}

internal interface IPreDecodeFilterSupport
{
    void SetPreDecodeFilter(int columnIndex, CSharpDB.Sql.BinaryOp op, DbValue literal);
    void SetPreDecodeFilter(in PreDecodeFilterSpec filter);
}

internal interface IEstimatedRowCountProvider
{
    int? EstimatedRowCount { get; }
}

internal interface IProjectionPushdownTarget
{
    bool TrySetOutputProjection(int[] columnIndices, ColumnDefinition[] outputSchema);
}

internal interface IEncodedPayloadSource
{
    ReadOnlyMemory<byte> CurrentPayload { get; }
}

internal interface IMaterializedRowsProvider
{
    bool TryTakeMaterializedRows(out List<DbValue[]> rows);
}

internal sealed class RowBatch
{
    private readonly DbValue[] _values;

    public RowBatch(int columnCount, int capacity)
    {
        if (columnCount < 0)
            throw new ArgumentOutOfRangeException(nameof(columnCount));
        if (capacity < 0)
            throw new ArgumentOutOfRangeException(nameof(capacity));

        ColumnCount = columnCount;
        Capacity = capacity;
        _values = columnCount == 0 || capacity == 0
            ? Array.Empty<DbValue>()
            : new DbValue[columnCount * capacity];
    }

    public int ColumnCount { get; }
    public int Capacity { get; }
    public int Count { get; private set; }
    public DbValue[] Values => _values;

    public void Reset()
    {
        Count = 0;
    }

    public void AppendRow(ReadOnlySpan<DbValue> values)
    {
        if (Count >= Capacity)
            throw new InvalidOperationException("The batch is already full.");

        CopyRowFrom(Count, values);
    }

    public void CopyRowFrom(int rowIndex, ReadOnlySpan<DbValue> values)
    {
        ValidateWritableRowIndex(rowIndex);
        if (values.Length != ColumnCount)
            throw new ArgumentException("Row width does not match batch column count.", nameof(values));

        values.CopyTo(GetWritableRowSpan(rowIndex));
        if (rowIndex >= Count)
            Count = rowIndex + 1;
    }

    public void CopyRowTo(int rowIndex, Span<DbValue> destination)
    {
        ValidateReadableRowIndex(rowIndex);
        if (destination.Length < ColumnCount)
            throw new ArgumentException("Destination span is too small for the requested row.", nameof(destination));

        GetRowSpan(rowIndex).CopyTo(destination);
    }

    public void CommitWrittenRow(int rowIndex)
    {
        ValidateWritableRowIndex(rowIndex);
        if (rowIndex >= Count)
            Count = rowIndex + 1;
    }

    public ReadOnlySpan<DbValue> GetRowSpan(int rowIndex)
    {
        ValidateReadableRowIndex(rowIndex);
        return GetWritableRowSpan(rowIndex);
    }

    public Span<DbValue> GetWritableRowSpan(int rowIndex)
    {
        ValidateWritableRowIndex(rowIndex);

        int offset = rowIndex * ColumnCount;
        return _values.AsSpan(offset, ColumnCount);
    }

    private void ValidateReadableRowIndex(int rowIndex)
    {
        if ((uint)rowIndex >= (uint)Count)
            throw new ArgumentOutOfRangeException(nameof(rowIndex));
    }

    private void ValidateWritableRowIndex(int rowIndex)
    {
        if ((uint)rowIndex >= (uint)Capacity)
            throw new ArgumentOutOfRangeException(nameof(rowIndex));
    }
}
