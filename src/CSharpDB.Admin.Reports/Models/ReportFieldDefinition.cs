using CSharpDB.Client.Models;

namespace CSharpDB.Admin.Reports.Models;

public sealed record ReportFieldDefinition(
    string Name,
    DbType DataType,
    bool IsNullable,
    bool IsReadOnly,
    string? DisplayName = null,
    IReadOnlyDictionary<string, object?>? Metadata = null);
