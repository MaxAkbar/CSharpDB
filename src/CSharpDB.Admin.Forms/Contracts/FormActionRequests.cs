namespace CSharpDB.Admin.Forms.Contracts;

public sealed record FormOpenRequest(
    string FormId,
    string FormName,
    IReadOnlyDictionary<string, object?> Arguments,
    string? Mode = null,
    object? RecordId = null,
    string? FilterExpression = null);

public sealed record FormCloseRequest(
    string? FormId,
    string? FormName);
