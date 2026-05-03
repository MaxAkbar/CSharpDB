namespace CSharpDB.Admin.Forms.Models;

public sealed record ControlRuleDefinition(
    string RuleId,
    string Condition,
    IReadOnlyList<ControlRuleEffect> Effects,
    string? Description = null);

public sealed record ControlRuleEffect(
    string ControlId,
    string Property,
    object? Value);
