namespace CSharpDB.Admin.Forms.Models;

public sealed record ValidationRule(
    string RuleId,
    string Message,
    IReadOnlyDictionary<string, object?> Parameters);
