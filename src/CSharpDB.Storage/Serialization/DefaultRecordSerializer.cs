using CSharpDB.Primitives;

namespace CSharpDB.Storage.Serialization;

/// <summary>
/// Adapter that preserves current static RecordEncoder behavior.
/// </summary>
public sealed class DefaultRecordSerializer : IRecordSerializer
{
    public byte[] Encode(ReadOnlySpan<DbValue> values) => RecordEncoder.Encode(values);

    public int GetEncodedLength(ReadOnlySpan<DbValue> values) => RecordEncoder.GetEncodedLength(values);

    public int EncodeInto(ReadOnlySpan<DbValue> values, Span<byte> destination, int encodedLength) =>
        RecordEncoder.EncodeInto(values, destination, encodedLength);

    public int EncodeInto(ReadOnlySpan<DbValue> values, Span<byte> destination) =>
        RecordEncoder.EncodeInto(values, destination);

    public DbValue[] Decode(ReadOnlySpan<byte> buffer) => RecordEncoder.Decode(buffer);

    public int DecodeInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination) =>
        RecordEncoder.DecodeInto(buffer, destination);

    public void DecodeSelectedInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination, ReadOnlySpan<int> selectedColumnIndices) =>
        RecordEncoder.DecodeSelectedInto(buffer, destination, selectedColumnIndices);

    public void DecodeSelectedCompactInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination, ReadOnlySpan<int> selectedColumnIndices) =>
        RecordEncoder.DecodeSelectedCompactInto(buffer, destination, selectedColumnIndices);

    public DbValue[] DecodeUpTo(ReadOnlySpan<byte> buffer, int maxColumnIndexInclusive) =>
        RecordEncoder.DecodeUpTo(buffer, maxColumnIndexInclusive);

    public DbValue DecodeColumn(ReadOnlySpan<byte> buffer, int columnIndex) =>
        RecordEncoder.DecodeColumn(buffer, columnIndex);

    public bool TryColumnTextEquals(ReadOnlySpan<byte> buffer, int columnIndex, ReadOnlySpan<byte> expectedUtf8, out bool equals) =>
        RecordEncoder.TryColumnTextEquals(buffer, columnIndex, expectedUtf8, out equals);

    public bool IsColumnNull(ReadOnlySpan<byte> buffer, int columnIndex) =>
        RecordEncoder.IsColumnNull(buffer, columnIndex);

    public bool TryDecodeNumericColumn(
        ReadOnlySpan<byte> buffer,
        int columnIndex,
        out long intValue,
        out double realValue,
        out bool isReal) =>
        RecordEncoder.TryDecodeNumericColumn(buffer, columnIndex, out intValue, out realValue, out isReal);
}
