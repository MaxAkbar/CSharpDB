using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Services;

public sealed class DefaultValidationInferenceService : IValidationInferenceService
{
    private static readonly HashSet<string> BuiltInRuleIds = new(StringComparer.OrdinalIgnoreCase)
    {
        "required",
        "maxLength",
        "range",
        "regex",
        "oneOf",
    };

    private readonly DbValidationRuleRegistry _validationRules;
    private readonly DbExtensionPolicy _callbackPolicy;

    public DefaultValidationInferenceService()
        : this(DbValidationRuleRegistry.Empty, DbExtensionPolicies.DefaultHostCallbackPolicy)
    {
    }

    public DefaultValidationInferenceService(
        DbValidationRuleRegistry validationRules,
        DbExtensionPolicy callbackPolicy)
    {
        _validationRules = validationRules;
        _callbackPolicy = callbackPolicy;
    }

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
        => EvaluateAsync(form, record).GetAwaiter().GetResult();

    public async Task<IReadOnlyList<ValidationError>> EvaluateAsync(
        FormDefinition form,
        IDictionary<string, object?> record,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(form);
        ArgumentNullException.ThrowIfNull(record);

        var errors = new List<ValidationError>();
        Dictionary<string, DbValue> recordValues = DbCommandArguments.FromObjectDictionary(
            new Dictionary<string, object?>(record, StringComparer.OrdinalIgnoreCase));

        foreach (ControlDefinition control in form.Controls)
        {
            if (control.Binding is null)
                continue;

            string fieldName = control.Binding.FieldName;
            record.TryGetValue(fieldName, out object? value);
            IReadOnlyList<string> disabledIds = control.ValidationOverride?.DisableRuleIds ?? [];

            if (control.ValidationOverride?.DisableInferredRules != true)
                AddInferredControlErrors(errors, control, fieldName, value, disabledIds);

            foreach (ValidationRule rule in control.ValidationOverride?.AddRules ?? [])
            {
                if (disabledIds.Contains(rule.RuleId, StringComparer.OrdinalIgnoreCase))
                    continue;

                if (ApplyBuiltInRule(errors, rule, fieldName, value))
                    continue;

                await InvokeValidationRuleAsync(
                    errors,
                    form,
                    control,
                    fieldName,
                    value,
                    recordValues,
                    rule,
                    DbValidationRuleScope.Field,
                    ct).ConfigureAwait(false);
            }
        }

        foreach (ValidationRule rule in form.ValidationRules ?? [])
        {
            await InvokeValidationRuleAsync(
                errors,
                form,
                control: null,
                fieldName: null,
                value: null,
                recordValues,
                rule,
                DbValidationRuleScope.Form,
                ct).ConfigureAwait(false);
        }

        return errors;
    }

    private static void AddInferredControlErrors(
        List<ValidationError> errors,
        ControlDefinition control,
        string fieldName,
        object? value,
        IReadOnlyList<string> disabledIds)
    {
        if (!disabledIds.Contains("required", StringComparer.OrdinalIgnoreCase) &&
            TryGetBoolean(control.Props.Values, "required", out bool required) &&
            required &&
            IsEmpty(value))
        {
            errors.Add(new ValidationError(fieldName, "required", $"{fieldName} is required."));
        }

        if (!disabledIds.Contains("maxLength", StringComparer.OrdinalIgnoreCase) &&
            TryGetLong(control.Props.Values, "maxLength", out long maxLength) &&
            value is string text &&
            text.Length > maxLength)
        {
            errors.Add(new ValidationError(fieldName, "maxLength", $"{fieldName} must be at most {maxLength} characters."));
        }

        if (!disabledIds.Contains("range", StringComparer.OrdinalIgnoreCase) &&
            TryGetDoubleValue(value, out double numericValue))
        {
            bool hasMin = TryGetDouble(control.Props.Values, "min", out double min);
            bool hasMax = TryGetDouble(control.Props.Values, "max", out double maxValue);
            if (hasMin && numericValue < min)
                errors.Add(new ValidationError(fieldName, "range", $"{fieldName} must be at least {min.ToString(CultureInfo.InvariantCulture)}."));
            if (hasMax && numericValue > maxValue)
                errors.Add(new ValidationError(fieldName, "range", $"{fieldName} must be at most {maxValue.ToString(CultureInfo.InvariantCulture)}."));
        }

        if (!disabledIds.Contains("regex", StringComparer.OrdinalIgnoreCase) &&
            TryGetString(control.Props.Values, "pattern", out string? pattern) &&
            pattern is not null &&
            value is string stringValue &&
            !Regex.IsMatch(stringValue, pattern))
        {
            errors.Add(new ValidationError(fieldName, "regex", $"{fieldName} has an invalid format."));
        }
    }

    private static bool ApplyBuiltInRule(
        List<ValidationError> errors,
        ValidationRule rule,
        string fieldName,
        object? value)
    {
        if (!BuiltInRuleIds.Contains(rule.RuleId))
            return false;

        switch (rule.RuleId)
        {
            case "required" when IsEmpty(value):
                errors.Add(new ValidationError(fieldName, rule.RuleId, GetRuleMessage(rule, $"{fieldName} is required.")));
                break;

            case "maxLength" when value is string text && TryGetLong(rule.Parameters, "max", out long maxLength) && text.Length > maxLength:
                errors.Add(new ValidationError(fieldName, rule.RuleId, GetRuleMessage(rule, $"{fieldName} must be at most {maxLength} characters.")));
                break;

            case "range" when TryGetDoubleValue(value, out double numericValue):
                bool hasMin = TryGetDouble(rule.Parameters, "min", out double min);
                bool hasMax = TryGetDouble(rule.Parameters, "max", out double maxValue);
                if ((hasMin && numericValue < min) || (hasMax && numericValue > maxValue))
                    errors.Add(new ValidationError(fieldName, rule.RuleId, GetRuleMessage(rule, $"{fieldName} is outside the allowed range.")));
                break;

            case "regex" when value is string stringValue && TryGetString(rule.Parameters, "pattern", out string? pattern) && pattern is not null && !Regex.IsMatch(stringValue, pattern):
                errors.Add(new ValidationError(fieldName, rule.RuleId, GetRuleMessage(rule, $"{fieldName} has an invalid format.")));
                break;

            case "oneOf" when !IsOneOfAllowedValue(value, rule.Parameters):
                errors.Add(new ValidationError(fieldName, rule.RuleId, GetRuleMessage(rule, $"{fieldName} must be one of the allowed values.")));
                break;
        }

        return true;
    }

    private async Task InvokeValidationRuleAsync(
        List<ValidationError> errors,
        FormDefinition form,
        ControlDefinition? control,
        string? fieldName,
        object? value,
        IReadOnlyDictionary<string, DbValue> recordValues,
        ValidationRule rule,
        DbValidationRuleScope scope,
        CancellationToken ct)
    {
        string ruleName = rule.RuleId?.Trim() ?? string.Empty;
        string defaultFieldName = fieldName ?? string.Empty;
        IReadOnlyDictionary<string, string> metadata = CreateValidationMetadata(form, control, ruleName, scope);

        if (string.IsNullOrWhiteSpace(ruleName))
        {
            errors.Add(new ValidationError(defaultFieldName, string.Empty, "Validation rule is missing a rule name."));
            return;
        }

        if (!_validationRules.TryGetRule(ruleName, out DbValidationRuleDefinition? definition))
        {
            string message = $"Validation rule '{ruleName}' is not registered in the current Admin host.";
            DbCallbackDiagnostics.WriteMissingValidationInvocation(ruleName, metadata, message);
            errors.Add(new ValidationError(defaultFieldName, ruleName, message));
            return;
        }

        var context = DbValidationRuleContext.Create(
            ruleName,
            scope,
            recordValues,
            DbCommandArguments.FromObjectDictionary(rule.Parameters),
            metadata) with
        {
            FormId = form.FormId,
            FormName = form.Name,
            TableName = form.TableName,
            ControlId = control?.ControlId,
            FieldName = fieldName,
            Value = DbCommandArguments.FromObject(value),
            FallbackMessage = rule.Message,
        };

        try
        {
            DbValidationRuleResult result = await definition
                .InvokeAsync(context, _callbackPolicy, DbExtensionHostMode.Embedded, ct)
                .ConfigureAwait(false);

            if (!result.Succeeded || result.Failures is { Count: > 0 })
                AddValidationRuleFailures(errors, result, defaultFieldName, ruleName, rule.Message);
        }
        catch (DbCallbackPolicyException ex)
        {
            string reason = ex.Decision.DenialReason ?? ex.Message;
            errors.Add(new ValidationError(defaultFieldName, ruleName, $"Validation rule '{ruleName}' was denied by policy: {reason}"));
        }
        catch (TimeoutException ex)
        {
            errors.Add(new ValidationError(defaultFieldName, ruleName, ex.Message));
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            errors.Add(new ValidationError(defaultFieldName, ruleName, $"Validation rule '{ruleName}' failed: {ex.Message}"));
        }
    }

    private static void AddValidationRuleFailures(
        List<ValidationError> errors,
        DbValidationRuleResult result,
        string defaultFieldName,
        string ruleName,
        string fallbackMessage)
    {
        if (result.Failures is { Count: > 0 } failures)
        {
            foreach (DbValidationFailure failure in failures)
            {
                string fieldName = failure.FieldName ?? string.Empty;
                errors.Add(new ValidationError(
                    fieldName,
                    failure.RuleId ?? ruleName,
                    string.IsNullOrWhiteSpace(failure.Message) ? GetFallbackFailureMessage(ruleName, fallbackMessage, result.Message) : failure.Message));
            }

            return;
        }

        errors.Add(new ValidationError(
            defaultFieldName,
            ruleName,
            GetFallbackFailureMessage(ruleName, fallbackMessage, result.Message)));
    }

    private static string GetFallbackFailureMessage(string ruleName, string fallbackMessage, string? resultMessage)
    {
        if (!string.IsNullOrWhiteSpace(resultMessage))
            return resultMessage;
        if (!string.IsNullOrWhiteSpace(fallbackMessage))
            return fallbackMessage;

        return $"Validation rule '{ruleName}' failed.";
    }

    private static IReadOnlyDictionary<string, string> CreateValidationMetadata(
        FormDefinition form,
        ControlDefinition? control,
        string ruleName,
        DbValidationRuleScope scope)
    {
        var metadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["surface"] = "admin.forms",
            ["ownerKind"] = "Form",
            ["ownerId"] = form.FormId,
            ["ownerName"] = form.Name,
            ["formId"] = form.FormId,
            ["formName"] = form.Name,
            ["tableName"] = form.TableName,
            ["validationScope"] = scope.ToString(),
            ["ruleName"] = ruleName,
            ["correlationId"] = Guid.NewGuid().ToString("N"),
        };

        if (scope == DbValidationRuleScope.Field && control is not null)
        {
            metadata["controlId"] = control.ControlId;
            if (control.Binding?.FieldName is { Length: > 0 } fieldName)
                metadata["fieldName"] = fieldName;
            metadata["location"] = $"controls.{control.ControlId}.validationRules.{ruleName}";
        }
        else
        {
            metadata["location"] = $"form.validationRules.{ruleName}";
        }

        return metadata;
    }

    private static string GetRuleMessage(ValidationRule rule, string fallback)
        => string.IsNullOrWhiteSpace(rule.Message) ? fallback : rule.Message;

    private static bool IsEmpty(object? value)
        => value is null or "" || value is string text && string.IsNullOrWhiteSpace(text);

    private static bool TryGetBoolean(IReadOnlyDictionary<string, object?> values, string key, out bool result)
    {
        result = false;
        if (!values.TryGetValue(key, out object? value) || value is null)
            return false;

        if (value is bool boolValue)
        {
            result = boolValue;
            return true;
        }

        if (value is JsonElement json)
        {
            if (json.ValueKind is JsonValueKind.True or JsonValueKind.False)
            {
                result = json.GetBoolean();
                return true;
            }

            if (json.ValueKind == JsonValueKind.String)
                value = json.GetString();
        }

        if (value is null)
            return false;

        string? text = value.ToString();
        return text is not null && bool.TryParse(text, out result);
    }

    private static bool TryGetString(IReadOnlyDictionary<string, object?> values, string key, out string? result)
    {
        result = null;
        if (!values.TryGetValue(key, out object? value) || value is null)
            return false;

        if (value is JsonElement json)
            value = json.ValueKind == JsonValueKind.String ? json.GetString() : json.ToString();

        result = value?.ToString();
        return !string.IsNullOrWhiteSpace(result);
    }

    private static bool TryGetLong(IReadOnlyDictionary<string, object?> values, string key, out long result)
    {
        result = 0;
        if (!values.TryGetValue(key, out object? value))
            return false;

        return TryGetLongValue(value, out result);
    }

    private static bool TryGetDouble(IReadOnlyDictionary<string, object?> values, string key, out double result)
    {
        result = 0;
        if (!values.TryGetValue(key, out object? value))
            return false;

        return TryGetDoubleValue(value, out result);
    }

    private static bool TryGetLongValue(object? value, out long result)
    {
        result = 0;
        if (value is null)
            return false;

        if (value is JsonElement json)
        {
            if (json.ValueKind == JsonValueKind.Number)
                return json.TryGetInt64(out result);
            if (json.ValueKind == JsonValueKind.String)
                value = json.GetString();
        }

        if (value is null)
            return false;

        if (value is IConvertible)
            return long.TryParse(Convert.ToString(value, CultureInfo.InvariantCulture), NumberStyles.Integer, CultureInfo.InvariantCulture, out result);

        string? text = value.ToString();
        return text is not null && long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out result);
    }

    private static bool TryGetDoubleValue(object? value, out double result)
    {
        result = 0;
        if (value is null)
            return false;

        if (value is JsonElement json)
        {
            if (json.ValueKind == JsonValueKind.Number)
                return json.TryGetDouble(out result);
            if (json.ValueKind == JsonValueKind.String)
                value = json.GetString();
        }

        if (value is null)
            return false;

        if (value is IConvertible)
            return double.TryParse(Convert.ToString(value, CultureInfo.InvariantCulture), NumberStyles.Float, CultureInfo.InvariantCulture, out result);

        string? text = value.ToString();
        return text is not null && double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out result);
    }

    private static bool IsOneOfAllowedValue(object? value, IReadOnlyDictionary<string, object?> parameters)
    {
        if (!parameters.TryGetValue("values", out object? rawValues) || rawValues is null)
            return true;

        string candidate = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        foreach (object? allowed in EnumerateValues(rawValues))
        {
            string allowedText = Convert.ToString(allowed, CultureInfo.InvariantCulture) ?? string.Empty;
            if (string.Equals(candidate, allowedText, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static IEnumerable<object?> EnumerateValues(object value)
    {
        if (value is JsonElement json && json.ValueKind == JsonValueKind.Array)
        {
            foreach (JsonElement item in json.EnumerateArray())
                yield return item.ValueKind == JsonValueKind.String ? item.GetString() : item.ToString();
            yield break;
        }

        if (value is IEnumerable<object?> objectValues)
        {
            foreach (object? item in objectValues)
                yield return item;
            yield break;
        }

        if (value is System.Collections.IEnumerable values && value is not string)
        {
            foreach (object? item in values)
                yield return item;
        }
    }
}
