using CSharpDB.Core;

namespace CSharpDB.Storage.Serialization;

/// <summary>
/// Binds a column ordinal once and exposes lightweight payload access helpers
/// for encoded SQL row records.
/// </summary>
public readonly struct RecordColumnAccessor
{
    public RecordColumnAccessor(int columnIndex)
    {
        if (columnIndex < 0)
            throw new ArgumentOutOfRangeException(nameof(columnIndex));

        ColumnIndex = columnIndex;
    }

    public int ColumnIndex { get; }

    public DbValue Decode(ReadOnlySpan<byte> buffer)
        => RecordEncoder.DecodeColumn(buffer, ColumnIndex);

    public bool TryTextEquals(ReadOnlySpan<byte> buffer, ReadOnlySpan<byte> expectedUtf8, out bool equals)
        => RecordEncoder.TryColumnTextEquals(buffer, ColumnIndex, expectedUtf8, out equals);

    public bool IsNull(ReadOnlySpan<byte> buffer)
        => RecordEncoder.IsColumnNull(buffer, ColumnIndex);

    public bool TryDecodeNumeric(
        ReadOnlySpan<byte> buffer,
        out long intValue,
        out double realValue,
        out bool isReal)
        => RecordEncoder.TryDecodeNumericColumn(buffer, ColumnIndex, out intValue, out realValue, out isReal);
}
