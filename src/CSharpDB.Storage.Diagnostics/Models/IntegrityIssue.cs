namespace CSharpDB.Storage.Diagnostics;

public sealed class IntegrityIssue
{
    public required string Code { get; init; }
    public required InspectSeverity Severity { get; init; }
    public required string Message { get; init; }
    public uint? PageId { get; init; }
    public long? Offset { get; init; }
}
