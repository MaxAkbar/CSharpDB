using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Services;

public sealed class DefaultValidationInferenceService : IValidationInferenceService
{
    public IReadOnlyList<ValidationRule> InferRules(FormFieldDefinition field)
    {
        var rules = new List<ValidationRule>();

        if (!field.IsNullable)
        {
            rules.Add(new ValidationRule(
                "required",
                $"{field.DisplayName ?? field.Name} is required.",
                new Dictionary<string, object?>()));
        }

        if (field.MaxLength is not null)
        {
            rules.Add(new ValidationRule(
                "maxLength",
                $"{field.DisplayName ?? field.Name} must be at most {field.MaxLength} characters.",
                new Dictionary<string, object?> { ["max"] = field.MaxLength }));
        }

        if (field.Min is not null || field.Max is not null)
        {
            string message = (field.Min, field.Max) switch
            {
                (not null, not null) => $"{field.DisplayName ?? field.Name} must be between {field.Min} and {field.Max}.",
                (not null, null) => $"{field.DisplayName ?? field.Name} must be at least {field.Min}.",
                (null, not null) => $"{field.DisplayName ?? field.Name} must be at most {field.Max}.",
                _ => string.Empty,
            };

            var parameters = new Dictionary<string, object?>();
            if (field.Min is not null)
                parameters["min"] = field.Min;
            if (field.Max is not null)
                parameters["max"] = field.Max;

            rules.Add(new ValidationRule("range", message, parameters));
        }

        if (field.Regex is not null)
        {
            rules.Add(new ValidationRule(
                "regex",
                $"{field.DisplayName ?? field.Name} has an invalid format.",
                new Dictionary<string, object?> { ["pattern"] = field.Regex }));
        }

        if (field.Choices is { Count: > 0 })
        {
            rules.Add(new ValidationRule(
                "oneOf",
                $"{field.DisplayName ?? field.Name} must be one of the allowed values.",
                new Dictionary<string, object?>
                {
                    ["values"] = field.Choices.Select(choice => choice.Value).ToArray(),
                }));
        }

        return rules;
    }

    public IReadOnlyList<ValidationError> Evaluate(FormDefinition form, IDictionary<string, object?> record)
    {
        var errors = new List<ValidationError>();

        foreach (var control in form.Controls)
        {
            if (control.Binding is null)
                continue;

            if (control.ValidationOverride?.DisableInferredRules == true)
                continue;

            string fieldName = control.Binding.FieldName;
            record.TryGetValue(fieldName, out object? value);
            var disabledIds = control.ValidationOverride?.DisableRuleIds ?? [];

            if (!disabledIds.Contains("maxLength") &&
                control.Props.Values.TryGetValue("maxLength", out object? maxLength) &&
                value is string text &&
                maxLength is long max &&
                text.Length > max)
            {
                errors.Add(new ValidationError(fieldName, "maxLength", $"{fieldName} must be at most {max} characters."));
            }

            if (control.ValidationOverride?.AddRules is not { } addedRules)
                continue;

            foreach (var rule in addedRules)
            {
                if (disabledIds.Contains(rule.RuleId))
                    continue;

                if (rule.RuleId == "required" && IsEmpty(value))
                    errors.Add(new ValidationError(fieldName, rule.RuleId, rule.Message));
            }
        }

        return errors;
    }

    private static bool IsEmpty(object? value)
        => value is null or "" || value is string text && string.IsNullOrWhiteSpace(text);
}
