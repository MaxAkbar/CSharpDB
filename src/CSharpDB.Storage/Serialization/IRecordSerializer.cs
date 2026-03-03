using CSharpDB.Core;

namespace CSharpDB.Storage.Serialization;

/// <summary>
/// Abstraction over row record encoding/decoding.
/// </summary>
public interface IRecordSerializer
{
    byte[] Encode(ReadOnlySpan<DbValue> values);
    DbValue[] Decode(ReadOnlySpan<byte> buffer);
    int DecodeInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination);
    void DecodeSelectedInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination, ReadOnlySpan<int> selectedColumnIndices);
    DbValue[] DecodeUpTo(ReadOnlySpan<byte> buffer, int maxColumnIndexInclusive);
    DbValue DecodeColumn(ReadOnlySpan<byte> buffer, int columnIndex);
    bool TryColumnTextEquals(ReadOnlySpan<byte> buffer, int columnIndex, ReadOnlySpan<byte> expectedUtf8, out bool equals);
    bool IsColumnNull(ReadOnlySpan<byte> buffer, int columnIndex);
    bool TryDecodeNumericColumn(
        ReadOnlySpan<byte> buffer,
        int columnIndex,
        out long intValue,
        out double realValue,
        out bool isReal);
}
