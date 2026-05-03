namespace CSharpDB.Admin.Forms.Contracts;

public sealed record ControlFilterState(
    string FilterExpression,
    IReadOnlyDictionary<string, object?> Parameters);
