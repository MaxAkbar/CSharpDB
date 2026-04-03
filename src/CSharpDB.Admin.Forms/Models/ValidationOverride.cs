namespace CSharpDB.Admin.Forms.Models;

public sealed record ValidationOverride(
    bool DisableInferredRules,
    IReadOnlyList<ValidationRule> AddRules,
    IReadOnlyList<string> DisableRuleIds);
