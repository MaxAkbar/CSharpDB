namespace CSharpDB.Migration.Files.Csv;

public static class CsvDiagnosticRules
{
    public const string MissingHeader = "MIG-CSV-HEADER-001";
    public const string HeaderWidthMismatch = "MIG-CSV-HEADER-002";
    public const string MalformedData = "MIG-CSV-FORMAT-001";
    public const string InvalidEncoding = "MIG-CSV-ENCODING-001";
    public const string FieldLimitExceeded = "MIG-CSV-LIMIT-FIELD-001";
    public const string RecordLimitExceeded = "MIG-CSV-LIMIT-RECORD-001";
    public const string FieldCountLimitExceeded = "MIG-CSV-LIMIT-COLUMNS-001";
    public const string ExtraFields = "MIG-CSV-SHAPE-001";
    public const string InspectionCharacterLimitExceeded = "MIG-CSV-INSPECTION-LIMIT-CHARACTERS-001";
}

public sealed class CsvReadDiagnostic
{
    internal CsvReadDiagnostic(
        string ruleId,
        string message,
        long? logicalRecordNumber = null,
        long? dataRecordNumber = null,
        long? startPhysicalLine = null,
        long? endPhysicalLine = null,
        int? columnIndex = null)
    {
        RuleId = ruleId;
        Message = message;
        LogicalRecordNumber = logicalRecordNumber;
        DataRecordNumber = dataRecordNumber;
        StartPhysicalLine = startPhysicalLine;
        EndPhysicalLine = endPhysicalLine;
        ColumnIndex = columnIndex;
    }

    public string RuleId { get; }

    /// <summary>A deterministic message that never includes source values.</summary>
    public string Message { get; }

    public long? LogicalRecordNumber { get; }

    public long? DataRecordNumber { get; }

    public long? StartPhysicalLine { get; }

    public long? EndPhysicalLine { get; }

    /// <summary>Zero-based column index when known.</summary>
    public int? ColumnIndex { get; }
}

public sealed class CsvReadException : Exception
{
    internal CsvReadException(CsvReadDiagnostic diagnostic)
        : base(diagnostic.Message)
    {
        Diagnostic = diagnostic;
    }

    public CsvReadDiagnostic Diagnostic { get; }
}
