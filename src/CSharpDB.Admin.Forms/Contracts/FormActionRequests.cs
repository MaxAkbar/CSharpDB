namespace CSharpDB.Admin.Forms.Contracts;

public sealed record FormOpenRequest(
    string FormId,
    string FormName,
    IReadOnlyDictionary<string, object?> Arguments);

public sealed record FormCloseRequest(
    string? FormId,
    string? FormName);
