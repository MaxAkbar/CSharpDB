using System.Text.Json;
using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Services;

public static class FormActionManifestValidator
{
    private static readonly HashSet<string> s_supportedControlProperties = new(StringComparer.OrdinalIgnoreCase)
    {
        "visible",
        "enabled",
        "readOnly",
        "required",
        "styleVariant",
        "validationMessage",
        "text",
        "value",
        "placeholder",
    };

    public static FormActionValidationResult Validate(
        FormDefinition form,
        FormActionValidationOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(form);

        options ??= new FormActionValidationOptions();
        FormActionRuntimeCapabilities capabilities = options.RuntimeCapabilities ?? FormActionRuntimeCapabilities.None;
        var issues = new List<FormActionValidationIssue>();
        var controlIds = new HashSet<string>(
            form.Controls.Select(static control => control.ControlId),
            StringComparer.OrdinalIgnoreCase);
        var sequenceNames = BuildSequenceNameIndex(form.ActionSequences, issues);
        HashSet<string>? availableForms = options.AvailableForms is null
            ? null
            : new HashSet<string>(options.AvailableForms, StringComparer.OrdinalIgnoreCase);
        HashSet<string>? availableProcedures = options.AvailableProcedures is null
            ? null
            : new HashSet<string>(options.AvailableProcedures, StringComparer.OrdinalIgnoreCase);

        foreach (FormEventBinding binding in form.EventBindings ?? [])
        {
            if (binding.ActionSequence is not null)
            {
                ValidateSequence(
                    binding.ActionSequence,
                    $"form.events.{binding.Event}.actionSequence",
                    binding.Event.ToString(),
                    controlIds,
                    sequenceNames,
                    availableForms,
                    availableProcedures,
                    options.Schema,
                    capabilities,
                    issues);
            }
        }

        foreach (ControlDefinition control in form.Controls)
        {
            foreach (ControlEventBinding binding in control.EventBindings ?? [])
            {
                if (binding.ActionSequence is not null)
                {
                    ValidateSequence(
                        binding.ActionSequence,
                        $"controls.{control.ControlId}.events.{binding.Event}.actionSequence",
                        binding.Event.ToString(),
                        controlIds,
                        sequenceNames,
                        availableForms,
                        availableProcedures,
                        options.Schema,
                        capabilities,
                        issues);
                }
            }
        }

        foreach (DbActionSequence sequence in form.ActionSequences ?? [])
        {
            string location = string.IsNullOrWhiteSpace(sequence.Name)
                ? "form.actionSequences.unnamed"
                : $"form.actionSequences.{sequence.Name}";
            ValidateSequence(
                sequence,
                location,
                eventName: null,
                controlIds,
                sequenceNames,
                availableForms,
                availableProcedures,
                options.Schema,
                capabilities,
                issues);
        }

        ValidateRules(form.Rules, controlIds, options.Schema, issues);

        return new FormActionValidationResult(
            !issues.Any(static issue => issue.Severity == FormActionValidationSeverity.Error),
            issues.ToArray());
    }

    private static Dictionary<string, int> BuildSequenceNameIndex(
        IReadOnlyList<DbActionSequence>? sequences,
        List<FormActionValidationIssue> issues)
    {
        var names = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (DbActionSequence sequence in sequences ?? [])
        {
            if (string.IsNullOrWhiteSpace(sequence.Name))
                continue;

            string name = sequence.Name.Trim();
            names[name] = names.TryGetValue(name, out int count) ? count + 1 : 1;
        }

        foreach ((string name, int count) in names.Where(static pair => pair.Value > 1))
        {
            issues.Add(new FormActionValidationIssue(
                FormActionValidationSeverity.Error,
                DbActionKind.RunActionSequence,
                "admin.forms",
                $"form.actionSequences.{name}",
                $"Form action sequence name '{name}' is ambiguous because {count} sequences use it.",
                Target: name,
                ActionSequence: name));
        }

        return names;
    }

    private static void ValidateSequence(
        DbActionSequence sequence,
        string sequenceLocation,
        string? eventName,
        HashSet<string> controlIds,
        Dictionary<string, int> sequenceNames,
        HashSet<string>? availableForms,
        HashSet<string>? availableProcedures,
        FormTableDefinition? schema,
        FormActionRuntimeCapabilities capabilities,
        List<FormActionValidationIssue> issues)
    {
        IReadOnlyList<DbActionStep> steps = sequence.Steps ?? [];
        for (int i = 0; i < steps.Count; i++)
        {
            DbActionStep step = steps[i];
            string location = $"{sequenceLocation}.steps[{i}]";
            ValidateStep(
                step,
                location,
                eventName,
                sequence.Name,
                i,
                controlIds,
                sequenceNames,
                availableForms,
                availableProcedures,
                schema,
                capabilities,
                issues);
        }
    }

    private static void ValidateStep(
        DbActionStep step,
        string location,
        string? eventName,
        string? actionSequence,
        int stepIndex,
        HashSet<string> controlIds,
        Dictionary<string, int> sequenceNames,
        HashSet<string>? availableForms,
        HashSet<string>? availableProcedures,
        FormTableDefinition? schema,
        FormActionRuntimeCapabilities capabilities,
        List<FormActionValidationIssue> issues)
    {
        switch (step.Kind)
        {
            case DbActionKind.RunCommand:
                if (string.IsNullOrWhiteSpace(step.CommandName))
                    AddError(issues, step, location, "RunCommand action requires a command name.", eventName, actionSequence, stepIndex);
                break;
            case DbActionKind.SetFieldValue:
                if (string.IsNullOrWhiteSpace(step.Target))
                    AddError(issues, step, location, "SetFieldValue action requires a target field.", eventName, actionSequence, stepIndex);
                break;
            case DbActionKind.RunActionSequence:
                ValidateRunActionSequence(step, location, eventName, actionSequence, stepIndex, sequenceNames, issues);
                break;
            case DbActionKind.OpenForm:
                RequireCapability(capabilities.OpenForm, issues, step, location, "OpenForm", eventName, actionSequence, stepIndex);
                ValidateOpenForm(step, location, eventName, actionSequence, stepIndex, availableForms, issues);
                break;
            case DbActionKind.CloseForm:
                RequireCapability(capabilities.CloseForm, issues, step, location, "CloseForm", eventName, actionSequence, stepIndex);
                break;
            case DbActionKind.ApplyFilter:
                RequireCapability(capabilities.ApplyFilter, issues, step, location, "ApplyFilter", eventName, actionSequence, stepIndex);
                ValidateApplyFilter(step, location, eventName, actionSequence, stepIndex, controlIds, schema, issues);
                break;
            case DbActionKind.ClearFilter:
                RequireCapability(capabilities.ClearFilter, issues, step, location, "ClearFilter", eventName, actionSequence, stepIndex);
                ValidateOptionalControlTarget(step, location, eventName, actionSequence, stepIndex, controlIds, issues);
                break;
            case DbActionKind.RunSql:
                RequireCapability(capabilities.RunSql, issues, step, location, "RunSql", eventName, actionSequence, stepIndex);
                if (string.IsNullOrWhiteSpace(ReadText(step.Value) ?? ReadText(step.Target) ?? ReadArgumentText(step.Arguments, "sql", "name")))
                    AddError(issues, step, location, "RunSql action requires SQL text or a named SQL operation.", eventName, actionSequence, stepIndex);
                break;
            case DbActionKind.RunProcedure:
                RequireCapability(capabilities.RunProcedure, issues, step, location, "RunProcedure", eventName, actionSequence, stepIndex);
                ValidateRunProcedure(step, location, eventName, actionSequence, stepIndex, availableProcedures, issues);
                break;
            case DbActionKind.SetControlProperty:
                RequireCapability(capabilities.SetControlProperty, issues, step, location, "SetControlProperty", eventName, actionSequence, stepIndex);
                ValidateControlProperty(step, location, eventName, actionSequence, stepIndex, controlIds, issues);
                break;
            case DbActionKind.SetControlVisibility:
            case DbActionKind.SetControlEnabled:
            case DbActionKind.SetControlReadOnly:
                RequireCapability(capabilities.SetControlProperty, issues, step, location, step.Kind.ToString(), eventName, actionSequence, stepIndex);
                ValidateRequiredControlTarget(step, location, eventName, actionSequence, stepIndex, controlIds, issues);
                break;
            case DbActionKind.NewRecord:
            case DbActionKind.SaveRecord:
            case DbActionKind.DeleteRecord:
            case DbActionKind.RefreshRecords:
            case DbActionKind.PreviousRecord:
            case DbActionKind.NextRecord:
            case DbActionKind.GoToRecord:
                RequireCapability(capabilities.RecordActions, issues, step, location, step.Kind.ToString(), eventName, actionSequence, stepIndex);
                break;
        }
    }

    private static void ValidateRunActionSequence(
        DbActionStep step,
        string location,
        string? eventName,
        string? actionSequence,
        int stepIndex,
        Dictionary<string, int> sequenceNames,
        List<FormActionValidationIssue> issues)
    {
        string? target = ReadSequenceName(step);
        if (string.IsNullOrWhiteSpace(target))
        {
            AddError(issues, step, location, "RunActionSequence action requires a sequence name.", eventName, actionSequence, stepIndex);
            return;
        }

        if (!sequenceNames.TryGetValue(target, out int count))
        {
            AddError(issues, step, location, $"Unknown form action sequence '{target}'.", eventName, actionSequence, stepIndex, target);
            return;
        }

        if (count > 1)
            AddError(issues, step, location, $"Form action sequence name '{target}' is ambiguous.", eventName, actionSequence, stepIndex, target);
    }

    private static void ValidateOpenForm(
        DbActionStep step,
        string location,
        string? eventName,
        string? actionSequence,
        int stepIndex,
        HashSet<string>? availableForms,
        List<FormActionValidationIssue> issues)
    {
        string? formName = ReadText(step.Target)
            ?? ReadText(step.Value)
            ?? ReadArgumentText(step.Arguments, "formName", "form", "name");
        if (string.IsNullOrWhiteSpace(formName))
        {
            AddError(issues, step, location, "OpenForm action requires a target form name.", eventName, actionSequence, stepIndex);
            return;
        }

        if (availableForms is not null && !availableForms.Contains(formName))
            AddError(issues, step, location, $"OpenForm target '{formName}' was not found.", eventName, actionSequence, stepIndex, formName);
    }

    private static void ValidateApplyFilter(
        DbActionStep step,
        string location,
        string? eventName,
        string? actionSequence,
        int stepIndex,
        HashSet<string> controlIds,
        FormTableDefinition? schema,
        List<FormActionValidationIssue> issues)
    {
        ValidateOptionalControlTarget(step, location, eventName, actionSequence, stepIndex, controlIds, issues);
        string? filter = ReadText(step.Value) ?? ReadArgumentText(step.Arguments, "filter", "where");
        string? target = ReadText(step.Target) ?? ReadArgumentText(step.Arguments, "target");
        bool targetsForm = string.IsNullOrWhiteSpace(target) ||
            string.Equals(target, "form", StringComparison.OrdinalIgnoreCase);
        FormTableDefinition? filterSchema = targetsForm ? schema : null;
        if (string.IsNullOrWhiteSpace(filter))
            AddError(issues, step, location, "ApplyFilter action requires a filter expression.", eventName, actionSequence, stepIndex);
        else if (!FormFilterExpression.TryParse(filter, filterSchema, out _, out string? filterError))
            AddError(issues, step, location, $"ApplyFilter expression '{filter}' is malformed: {filterError}", eventName, actionSequence, stepIndex);
    }

    private static void ValidateRunProcedure(
        DbActionStep step,
        string location,
        string? eventName,
        string? actionSequence,
        int stepIndex,
        HashSet<string>? availableProcedures,
        List<FormActionValidationIssue> issues)
    {
        string? procedureName = ReadText(step.Target)
            ?? ReadText(step.Value)
            ?? ReadArgumentText(step.Arguments, "procedureName", "procedure", "name");
        if (string.IsNullOrWhiteSpace(procedureName))
        {
            AddError(issues, step, location, "RunProcedure action requires a procedure name.", eventName, actionSequence, stepIndex);
            return;
        }

        if (availableProcedures is not null && !availableProcedures.Contains(procedureName))
            AddError(issues, step, location, $"Procedure '{procedureName}' was not found.", eventName, actionSequence, stepIndex, procedureName);
    }

    private static void ValidateControlProperty(
        DbActionStep step,
        string location,
        string? eventName,
        string? actionSequence,
        int stepIndex,
        HashSet<string> controlIds,
        List<FormActionValidationIssue> issues)
    {
        ValidateRequiredControlTarget(step, location, eventName, actionSequence, stepIndex, controlIds, issues);
        string? property = ReadArgumentText(step.Arguments, "property", "propertyName");
        if (string.IsNullOrWhiteSpace(property))
        {
            AddError(issues, step, location, "SetControlProperty action requires a property name.", eventName, actionSequence, stepIndex);
            return;
        }

        if (!s_supportedControlProperties.Contains(property))
            AddError(issues, step, location, $"Control property '{property}' is not supported.", eventName, actionSequence, stepIndex, step.Target);
    }

    private static void ValidateRequiredControlTarget(
        DbActionStep step,
        string location,
        string? eventName,
        string? actionSequence,
        int stepIndex,
        HashSet<string> controlIds,
        List<FormActionValidationIssue> issues)
    {
        if (string.IsNullOrWhiteSpace(step.Target))
        {
            AddError(issues, step, location, $"{step.Kind} action requires a target control id.", eventName, actionSequence, stepIndex);
            return;
        }

        if (!controlIds.Contains(step.Target))
            AddError(issues, step, location, $"Unknown control '{step.Target}'.", eventName, actionSequence, stepIndex, step.Target);
    }

    private static void ValidateOptionalControlTarget(
        DbActionStep step,
        string location,
        string? eventName,
        string? actionSequence,
        int stepIndex,
        HashSet<string> controlIds,
        List<FormActionValidationIssue> issues)
    {
        if (string.IsNullOrWhiteSpace(step.Target) ||
            string.Equals(step.Target, "form", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (!controlIds.Contains(step.Target))
            AddError(issues, step, location, $"Unknown control '{step.Target}'.", eventName, actionSequence, stepIndex, step.Target);
    }

    private static void ValidateRules(
        IReadOnlyList<ControlRuleDefinition>? rules,
        HashSet<string> controlIds,
        FormTableDefinition? schema,
        List<FormActionValidationIssue> issues)
    {
        foreach (ControlRuleDefinition rule in rules ?? [])
        {
            string ruleId = string.IsNullOrWhiteSpace(rule.RuleId) ? "unnamed" : rule.RuleId.Trim();
            string location = $"form.rules.{ruleId}";
            if (string.IsNullOrWhiteSpace(rule.RuleId))
            {
                issues.Add(new FormActionValidationIssue(
                    FormActionValidationSeverity.Error,
                    DbActionKind.SetControlProperty,
                    "admin.forms",
                    location,
                    "Control rule requires a rule id."));
            }

            if (string.IsNullOrWhiteSpace(rule.Condition))
            {
                issues.Add(new FormActionValidationIssue(
                    FormActionValidationSeverity.Error,
                    DbActionKind.SetControlProperty,
                    "admin.forms",
                    location,
                    $"Control rule '{ruleId}' requires a condition."));
            }
            else if (schema is not null)
            {
                var values = schema.Fields.ToDictionary(
                    static field => field.Name,
                    static _ => (object?)null,
                    StringComparer.OrdinalIgnoreCase);
                if (!FormActionConditionEvaluator.TryEvaluate(rule.Condition, values, null, null, null, out _, out string? conditionError))
                {
                    issues.Add(new FormActionValidationIssue(
                        FormActionValidationSeverity.Error,
                        DbActionKind.SetControlProperty,
                        "admin.forms",
                        location,
                        $"Control rule '{ruleId}' condition is malformed: {conditionError}"));
                }
            }

            if (rule.Effects.Count == 0)
            {
                issues.Add(new FormActionValidationIssue(
                    FormActionValidationSeverity.Error,
                    DbActionKind.SetControlProperty,
                    "admin.forms",
                    location,
                    $"Control rule '{ruleId}' requires at least one effect."));
            }

            for (int i = 0; i < rule.Effects.Count; i++)
            {
                ControlRuleEffect effect = rule.Effects[i];
                string effectLocation = $"{location}.effects[{i}]";
                if (string.IsNullOrWhiteSpace(effect.ControlId) || !controlIds.Contains(effect.ControlId))
                {
                    issues.Add(new FormActionValidationIssue(
                        FormActionValidationSeverity.Error,
                        DbActionKind.SetControlProperty,
                        "admin.forms",
                        effectLocation,
                        $"Control rule '{ruleId}' targets unknown control '{effect.ControlId}'.",
                        Target: effect.ControlId));
                }

                if (string.IsNullOrWhiteSpace(effect.Property) || !s_supportedControlProperties.Contains(effect.Property))
                {
                    issues.Add(new FormActionValidationIssue(
                        FormActionValidationSeverity.Error,
                        DbActionKind.SetControlProperty,
                        "admin.forms",
                        effectLocation,
                        $"Control rule '{ruleId}' uses unsupported property '{effect.Property}'.",
                        Target: effect.ControlId));
                }
            }
        }
    }

    private static void RequireCapability(
        bool capability,
        List<FormActionValidationIssue> issues,
        DbActionStep step,
        string location,
        string actionName,
        string? eventName,
        string? actionSequence,
        int stepIndex)
    {
        if (capability)
            return;

        issues.Add(new FormActionValidationIssue(
            FormActionValidationSeverity.Warning,
            step.Kind,
            "admin.forms",
            location,
            $"{actionName} action requires a rendered form runtime capability.",
            Target: step.Target,
            EventName: eventName,
            ActionSequence: actionSequence,
            StepIndex: stepIndex));
    }

    private static void AddError(
        List<FormActionValidationIssue> issues,
        DbActionStep step,
        string location,
        string message,
        string? eventName,
        string? actionSequence,
        int stepIndex,
        string? target = null)
        => issues.Add(new FormActionValidationIssue(
            FormActionValidationSeverity.Error,
            step.Kind,
            "admin.forms",
            location,
            message,
            Target: target ?? step.Target,
            EventName: eventName,
            ActionSequence: actionSequence,
            StepIndex: stepIndex));

    private static string? ReadSequenceName(DbActionStep step)
    {
        if (!string.IsNullOrWhiteSpace(step.SequenceName))
            return step.SequenceName.Trim();

        if (!string.IsNullOrWhiteSpace(step.Target))
            return step.Target.Trim();

        return ReadArgumentText(step.Arguments, "sequenceName", "sequence", "name");
    }

    private static string? ReadText(object? value)
        => NormalizeValue(value)?.ToString()?.Trim();

    private static string? ReadArgumentText(
        IReadOnlyDictionary<string, object?>? arguments,
        params string[] keys)
    {
        if (arguments is null)
            return null;

        foreach (string key in keys)
        {
            if (arguments.TryGetValue(key, out object? value))
            {
                string? text = ReadText(value);
                if (!string.IsNullOrWhiteSpace(text))
                    return text;
            }
        }

        return null;
    }

    private static object? NormalizeValue(object? value)
        => value is JsonElement json ? NormalizeJsonValue(json) : value;

    private static object? NormalizeJsonValue(JsonElement value)
        => value.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => value.TryGetInt64(out long integer) ? integer : value.GetDouble(),
            _ => value.ToString(),
        };

    private static bool HasBalancedBracketsAndQuotes(string filter)
    {
        int bracketDepth = 0;
        bool inString = false;
        for (int i = 0; i < filter.Length; i++)
        {
            char ch = filter[i];
            if (ch == '\'')
            {
                inString = !inString;
                continue;
            }

            if (inString)
                continue;

            if (ch == '[')
            {
                bracketDepth++;
                continue;
            }

            if (ch == ']')
            {
                bracketDepth--;
                if (bracketDepth < 0)
                    return false;
            }
        }

        return bracketDepth == 0 && !inString;
    }
}
