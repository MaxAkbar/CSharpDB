using System.Collections.ObjectModel;

namespace CSharpDB.Migration.Files.Csv;

public enum CsvFieldKind
{
    Text,
    Empty,
    Null,
    Missing,
}

public sealed class CsvLogicalField
{
    internal CsvLogicalField(
        int columnIndex,
        CsvFieldKind kind,
        string? value,
        bool wasQuoted)
    {
        ColumnIndex = columnIndex;
        Kind = kind;
        Value = value;
        WasQuoted = wasQuoted;
    }

    /// <summary>Zero-based column index.</summary>
    public int ColumnIndex { get; }

    public CsvFieldKind Kind { get; }

    /// <summary>
    /// Exact decoded text for <see cref="CsvFieldKind.Text"/>, an empty string
    /// for <see cref="CsvFieldKind.Empty"/>, and null otherwise.
    /// </summary>
    public string? Value { get; }

    public bool WasQuoted { get; }
}

public sealed class CsvHeader
{
    internal CsvHeader(
        long logicalRecordNumber,
        long startPhysicalLine,
        long endPhysicalLine,
        string[] fields,
        bool[] quotedFields)
    {
        LogicalRecordNumber = logicalRecordNumber;
        StartPhysicalLine = startPhysicalLine;
        EndPhysicalLine = endPhysicalLine;
        Fields = Array.AsReadOnly(fields);
        QuotedFields = Array.AsReadOnly(quotedFields);
    }

    public long LogicalRecordNumber { get; }

    public long StartPhysicalLine { get; }

    public long EndPhysicalLine { get; }

    public ReadOnlyCollection<string> Fields { get; }

    public ReadOnlyCollection<bool> QuotedFields { get; }
}

public sealed class CsvLogicalRecord
{
    internal CsvLogicalRecord(
        long logicalRecordNumber,
        long dataRecordNumber,
        long startPhysicalLine,
        long endPhysicalLine,
        int presentFieldCount,
        CsvLogicalField[] fields)
    {
        LogicalRecordNumber = logicalRecordNumber;
        DataRecordNumber = dataRecordNumber;
        StartPhysicalLine = startPhysicalLine;
        EndPhysicalLine = endPhysicalLine;
        PresentFieldCount = presentFieldCount;
        Fields = Array.AsReadOnly(fields);
    }

    /// <summary>One-based record number including an optional header.</summary>
    public long LogicalRecordNumber { get; }

    /// <summary>One-based data-record number excluding the header.</summary>
    public long DataRecordNumber { get; }

    public long StartPhysicalLine { get; }

    public long EndPhysicalLine { get; }

    /// <summary>
    /// Number of fields physically present before missing fields were padded.
    /// This preserves the difference between a missing field and a present,
    /// empty final field.
    /// </summary>
    public int PresentFieldCount { get; }

    public ReadOnlyCollection<CsvLogicalField> Fields { get; }
}
