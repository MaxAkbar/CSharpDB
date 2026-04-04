namespace CSharpDB.Admin.Reports.Models;

public sealed record ReportSourceDefinition(
    ReportSourceKind Kind,
    string Name,
    string DisplayName,
    string BaseSql,
    string SourceSchemaSignature,
    IReadOnlyList<ReportFieldDefinition> Fields);
