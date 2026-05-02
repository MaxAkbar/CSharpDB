using CSharpDB.Admin.Forms.Models;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Services;

public static class FormAutomationMetadata
{
    private const string Surface = "admin.forms";
    private static readonly string[] IgnoredFormulaFunctions = ["SUM", "COUNT", "AVG", "MIN", "MAX"];
    private static readonly HashSet<string> BuiltInValidationRuleIds = new(StringComparer.OrdinalIgnoreCase)
    {
        "required",
        "maxLength",
        "range",
        "regex",
        "oneOf",
    };

    public static FormDefinition NormalizeForExport(FormDefinition form)
    {
        ArgumentNullException.ThrowIfNull(form);

        DbAutomationMetadata metadata = Build(form);
        return form with { Automation = metadata.IsEmpty ? null : metadata };
    }

    public static DbAutomationMetadata Build(FormDefinition form)
    {
        ArgumentNullException.ThrowIfNull(form);

        var builder = new DbAutomationMetadataBuilder();
        foreach (FormEventBinding binding in form.EventBindings ?? [])
        {
            string bindingLocation = $"form.events.{binding.Event}";
            builder.AddCommand(binding.CommandName, Surface, bindingLocation);
            AddActionSequence(builder, binding.ActionSequence, bindingLocation);
        }

        foreach (DbActionSequence sequence in form.ActionSequences ?? [])
        {
            string sequenceLocation = string.IsNullOrWhiteSpace(sequence.Name)
                ? "form.actionSequences.unnamed"
                : $"form.actionSequences.{sequence.Name}";
            AddActionSequence(builder, sequence, sequenceLocation);
        }

        AddValidationRules(builder, form.ValidationRules, "form.validationRules");

        foreach (ControlDefinition control in form.Controls)
        {
            AddCommandButton(builder, control);
            AddComputedFormula(builder, control);
            AddValidationRules(builder, control.ValidationOverride?.AddRules, $"controls.{control.ControlId}.validationRules");
            foreach (ControlEventBinding binding in control.EventBindings ?? [])
            {
                string bindingLocation = $"controls.{control.ControlId}.events.{binding.Event}";
                builder.AddCommand(binding.CommandName, Surface, bindingLocation);
                AddActionSequence(builder, binding.ActionSequence, bindingLocation);
            }
        }

        return builder.Build();
    }

    private static void AddCommandButton(DbAutomationMetadataBuilder builder, ControlDefinition control)
    {
        if (!string.Equals(control.ControlType, "commandButton", StringComparison.OrdinalIgnoreCase))
            return;

        if (control.Props.Values.TryGetValue("commandName", out object? commandName))
            builder.AddCommand(commandName?.ToString(), Surface, $"controls.{control.ControlId}.commandButton.click");
    }

    private static void AddComputedFormula(DbAutomationMetadataBuilder builder, ControlDefinition control)
    {
        if (!string.Equals(control.ControlType, "computed", StringComparison.OrdinalIgnoreCase))
            return;

        if (!control.Props.Values.TryGetValue("formula", out object? formula) || formula is null)
            return;

        AddScalarFunctions(builder, formula.ToString(), $"controls.{control.ControlId}.formula");
    }

    private static void AddActionSequence(
        DbAutomationMetadataBuilder builder,
        DbActionSequence? sequence,
        string bindingLocation)
    {
        if (sequence is null)
            return;

        string sequenceLocation = string.IsNullOrWhiteSpace(sequence.Name)
            ? $"{bindingLocation}.actionSequence"
            : $"{bindingLocation}.actionSequence.{sequence.Name}";
        for (int i = 0; i < sequence.Steps.Count; i++)
        {
            DbActionStep step = sequence.Steps[i];
            if (step.Kind == DbActionKind.RunCommand)
                builder.AddCommand(step.CommandName, Surface, $"{sequenceLocation}.steps[{i}]");
        }
    }

    private static void AddValidationRules(
        DbAutomationMetadataBuilder builder,
        IReadOnlyList<ValidationRule>? rules,
        string locationPrefix)
    {
        foreach (ValidationRule rule in rules ?? [])
        {
            if (string.IsNullOrWhiteSpace(rule.RuleId) ||
                BuiltInValidationRuleIds.Contains(rule.RuleId))
            {
                continue;
            }

            builder.AddValidationRule(rule.RuleId, Surface, $"{locationPrefix}.{rule.RuleId}");
        }
    }

    private static void AddScalarFunctions(DbAutomationMetadataBuilder builder, string? expression, string location)
    {
        foreach (DbAutomationScalarFunctionCall call in
            DbAutomationExpressionInspector.FindScalarFunctionCalls(expression, IgnoredFormulaFunctions))
        {
            builder.AddScalarFunction(call.Name, call.Arity, Surface, location);
        }
    }
}
