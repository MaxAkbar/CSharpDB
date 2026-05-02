using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Tests.Services;

public class DefaultValidationInferenceServiceTests
{
    private readonly DefaultValidationInferenceService _service = new();

    [Fact]
    public void InferRules_NonNullableField_ProducesRequiredRule()
    {
        var field = new FormFieldDefinition("Name", FieldDataType.String, false, false);

        IReadOnlyList<ValidationRule> rules = _service.InferRules(field);

        Assert.Contains(rules, rule => rule.RuleId == "required");
    }

    [Fact]
    public void InferRules_NullableField_NoRequiredRule()
    {
        var field = new FormFieldDefinition("Notes", FieldDataType.String, true, false);

        IReadOnlyList<ValidationRule> rules = _service.InferRules(field);

        Assert.DoesNotContain(rules, rule => rule.RuleId == "required");
    }

    [Fact]
    public void InferRules_WithMaxLength_ProducesMaxLengthRule()
    {
        var field = new FormFieldDefinition("Name", FieldDataType.String, false, false, MaxLength: 50);

        IReadOnlyList<ValidationRule> rules = _service.InferRules(field);

        ValidationRule rule = Assert.Single(rules, item => item.RuleId == "maxLength");
        Assert.Equal(50, rule.Parameters["max"]);
    }

    [Fact]
    public void InferRules_WithMinMax_ProducesRangeRule()
    {
        var field = new FormFieldDefinition("Quantity", FieldDataType.Int32, false, false, Min: 1, Max: 100);

        IReadOnlyList<ValidationRule> rules = _service.InferRules(field);

        ValidationRule rule = Assert.Single(rules, item => item.RuleId == "range");
        Assert.Equal(1m, rule.Parameters["min"]);
        Assert.Equal(100m, rule.Parameters["max"]);
    }

    [Fact]
    public void InferRules_WithRegex_ProducesRegexRule()
    {
        var field = new FormFieldDefinition("Email", FieldDataType.String, true, false, Regex: @"^.+@.+$");

        IReadOnlyList<ValidationRule> rules = _service.InferRules(field);

        ValidationRule rule = Assert.Single(rules, item => item.RuleId == "regex");
        Assert.Equal(@"^.+@.+$", rule.Parameters["pattern"]);
    }

    [Fact]
    public void InferRules_WithChoices_ProducesOneOfRule()
    {
        var field = new FormFieldDefinition(
            "Status",
            FieldDataType.String,
            false,
            false,
            Choices: [new EnumChoice("A", "Active"), new EnumChoice("I", "Inactive")]);

        IReadOnlyList<ValidationRule> rules = _service.InferRules(field);

        Assert.Contains(rules, rule => rule.RuleId == "oneOf");
    }

    [Fact]
    public void InferRules_NullableFieldWithoutConstraints_NoRules()
    {
        var field = new FormFieldDefinition("Notes", FieldDataType.String, true, false);

        IReadOnlyList<ValidationRule> rules = _service.InferRules(field);

        Assert.Empty(rules);
    }

    [Fact]
    public void Evaluate_WithAddedRequiredRule_ReturnsValidationError()
    {
        FormDefinition form = CreateForm(new ControlDefinition(
            "c1",
            "text",
            new Rect(0, 0, 100, 30),
            new BindingDefinition("Name", "TwoWay"),
            PropertyBag.Empty,
            new ValidationOverride(
                DisableInferredRules: false,
                AddRules: [new ValidationRule("required", "Name is required.", new Dictionary<string, object?>())],
                DisableRuleIds: [])));

        IReadOnlyList<ValidationError> errors = _service.Evaluate(
            form,
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase));

        ValidationError error = Assert.Single(errors);
        Assert.Equal("Name", error.FieldName);
        Assert.Equal("required", error.RuleId);
    }

    [Fact]
    public void Evaluate_WithMaxLengthProp_ReturnsValidationError()
    {
        FormDefinition form = CreateForm(new ControlDefinition(
            "c1",
            "text",
            new Rect(0, 0, 100, 30),
            new BindingDefinition("Name", "TwoWay"),
            new PropertyBag(new Dictionary<string, object?> { ["maxLength"] = 5L }),
            new ValidationOverride(false, [], [])));

        IReadOnlyList<ValidationError> errors = _service.Evaluate(
            form,
            new Dictionary<string, object?> { ["Name"] = "Too long" });

        ValidationError error = Assert.Single(errors);
        Assert.Equal("Name", error.FieldName);
        Assert.Equal("maxLength", error.RuleId);
    }

    [Fact]
    public void Evaluate_WithDisabledRequiredRule_SuppressesValidationError()
    {
        FormDefinition form = CreateForm(new ControlDefinition(
            "c1",
            "text",
            new Rect(0, 0, 100, 30),
            new BindingDefinition("Name", "TwoWay"),
            PropertyBag.Empty,
            new ValidationOverride(
                DisableInferredRules: false,
                AddRules: [new ValidationRule("required", "Name is required.", new Dictionary<string, object?>())],
                DisableRuleIds: ["required"])));

        IReadOnlyList<ValidationError> errors = _service.Evaluate(
            form,
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase));

        Assert.Empty(errors);
    }

    [Fact]
    public async Task EvaluateAsync_FieldCallbackFailure_ReturnsFieldValidationError()
    {
        DbValidationRuleRegistry registry = DbValidationRuleRegistry.Create(builder =>
            builder.AddRule(
                "BlockName",
                static (context, _) =>
                {
                    Assert.Equal("form-1", context.FormId);
                    Assert.Equal("Test Form", context.FormName);
                    Assert.Equal("Customers", context.TableName);
                    Assert.Equal("c1", context.ControlId);
                    Assert.Equal("Name", context.FieldName);
                    Assert.Equal("blocked", context.Parameters["mode"].AsText);
                    Assert.True(context.Record.ContainsKey("Name"));

                    return ValueTask.FromResult(
                        context.Value.Type == DbType.Text && context.Value.AsText == "Blocked"
                            ? DbValidationRuleResult.Failure("Name is blocked.", context.FieldName, context.RuleName)
                            : DbValidationRuleResult.Success());
                }));
        var service = new DefaultValidationInferenceService(registry, DbExtensionPolicies.DefaultHostCallbackPolicy);
        FormDefinition form = CreateForm(new ControlDefinition(
            "c1",
            "text",
            new Rect(0, 0, 100, 30),
            new BindingDefinition("Name", "TwoWay"),
            PropertyBag.Empty,
            new ValidationOverride(
                DisableInferredRules: false,
                AddRules:
                [
                    new ValidationRule(
                        "BlockName",
                        "Name failed validation.",
                        new Dictionary<string, object?> { ["mode"] = "blocked" }),
                ],
                DisableRuleIds: [])));

        IReadOnlyList<ValidationError> errors = await service.EvaluateAsync(
            form,
            new Dictionary<string, object?> { ["Name"] = "Blocked" },
            TestContext.Current.CancellationToken);

        ValidationError error = Assert.Single(errors);
        Assert.Equal("Name", error.FieldName);
        Assert.Equal("BlockName", error.RuleId);
        Assert.Equal("Name is blocked.", error.Message);
    }

    [Fact]
    public async Task EvaluateAsync_FormCallbackCanReturnFieldAndGlobalFailures()
    {
        DbValidationRuleRegistry registry = DbValidationRuleRegistry.Create(builder =>
            builder.AddRule(
                "DateRange",
                static (context, _) =>
                {
                    Assert.Equal(DbValidationRuleScope.Form, context.Scope);
                    Assert.Null(context.ControlId);
                    Assert.Null(context.FieldName);

                    string start = context.Record["StartDate"].AsText;
                    string end = context.Record["EndDate"].AsText;
                    return ValueTask.FromResult(string.CompareOrdinal(start, end) <= 0
                        ? DbValidationRuleResult.Success()
                        : DbValidationRuleResult.Failure(
                            [
                                new DbValidationFailure("EndDate", "End date must be after start date.", "DateRange"),
                                new DbValidationFailure(null, "Fix the date range before saving.", "DateRange"),
                            ]));
                }));
        var service = new DefaultValidationInferenceService(registry, DbExtensionPolicies.DefaultHostCallbackPolicy);
        FormDefinition form = CreateForm(
            new ControlDefinition("start", "text", new Rect(0, 0, 100, 30), new BindingDefinition("StartDate", "TwoWay"), PropertyBag.Empty, null),
            new ControlDefinition("end", "text", new Rect(0, 40, 100, 30), new BindingDefinition("EndDate", "TwoWay"), PropertyBag.Empty, null)) with
        {
            ValidationRules =
            [
                new ValidationRule("DateRange", "Date range is invalid.", new Dictionary<string, object?>()),
            ],
        };

        IReadOnlyList<ValidationError> errors = await service.EvaluateAsync(
            form,
            new Dictionary<string, object?>
            {
                ["StartDate"] = "2026-05-02",
                ["EndDate"] = "2026-05-01",
            },
            TestContext.Current.CancellationToken);

        Assert.Equal(2, errors.Count);
        Assert.Contains(errors, error => error.FieldName == "EndDate" && error.Message == "End date must be after start date.");
        Assert.Contains(errors, error => error.FieldName == string.Empty && error.Message == "Fix the date range before saving.");
    }

    [Fact]
    public async Task EvaluateAsync_MissingCallbackBlocksSave()
    {
        var service = new DefaultValidationInferenceService(DbValidationRuleRegistry.Empty, DbExtensionPolicies.DefaultHostCallbackPolicy);
        FormDefinition form = CreateForm(new ControlDefinition(
            "c1",
            "text",
            new Rect(0, 0, 100, 30),
            new BindingDefinition("Name", "TwoWay"),
            PropertyBag.Empty,
            new ValidationOverride(false, [new ValidationRule("MissingRule", "Fallback", new Dictionary<string, object?>())], [])));

        IReadOnlyList<ValidationError> errors = await service.EvaluateAsync(
            form,
            new Dictionary<string, object?> { ["Name"] = "Alice" },
            TestContext.Current.CancellationToken);

        ValidationError error = Assert.Single(errors);
        Assert.Equal("Name", error.FieldName);
        Assert.Equal("MissingRule", error.RuleId);
        Assert.Contains("not registered", error.Message);
    }

    [Fact]
    public async Task EvaluateAsync_DeniedCallbackBlocksSave()
    {
        DbValidationRuleRegistry registry = DbValidationRuleRegistry.Create(builder =>
            builder.AddRule("DenyMe", static (_, _) => ValueTask.FromResult(DbValidationRuleResult.Success())));
        var policy = new DbExtensionPolicy(
            AllowExtensions: true,
            Grants:
            [
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.ValidationRules,
                    DbExtensionCapabilityGrantStatus.Denied,
                    Reason: "Validation rules are disabled."),
            ]);
        var service = new DefaultValidationInferenceService(registry, policy);
        FormDefinition form = CreateForm(new ControlDefinition(
            "c1",
            "text",
            new Rect(0, 0, 100, 30),
            new BindingDefinition("Name", "TwoWay"),
            PropertyBag.Empty,
            new ValidationOverride(false, [new ValidationRule("DenyMe", "Fallback", new Dictionary<string, object?>())], [])));

        IReadOnlyList<ValidationError> errors = await service.EvaluateAsync(
            form,
            new Dictionary<string, object?> { ["Name"] = "Alice" },
            TestContext.Current.CancellationToken);

        ValidationError error = Assert.Single(errors);
        Assert.Equal("Name", error.FieldName);
        Assert.Contains("denied by policy", error.Message);
        Assert.Contains("Validation rules are disabled.", error.Message);
    }

    [Fact]
    public async Task EvaluateAsync_ThrownCallbackBlocksSave()
    {
        DbValidationRuleRegistry registry = DbValidationRuleRegistry.Create(builder =>
            builder.AddRule(
                "Throws",
                static (_, _) => throw new InvalidOperationException("callback broke")));
        var service = new DefaultValidationInferenceService(registry, DbExtensionPolicies.DefaultHostCallbackPolicy);
        FormDefinition form = CreateForm(new ControlDefinition(
            "c1",
            "text",
            new Rect(0, 0, 100, 30),
            new BindingDefinition("Name", "TwoWay"),
            PropertyBag.Empty,
            new ValidationOverride(false, [new ValidationRule("Throws", "Fallback", new Dictionary<string, object?>())], [])));

        IReadOnlyList<ValidationError> errors = await service.EvaluateAsync(
            form,
            new Dictionary<string, object?> { ["Name"] = "Alice" },
            TestContext.Current.CancellationToken);

        ValidationError error = Assert.Single(errors);
        Assert.Equal("Name", error.FieldName);
        Assert.Contains("callback broke", error.Message);
    }

    [Fact]
    public async Task EvaluateAsync_TimedOutCallbackBlocksSave()
    {
        DbValidationRuleRegistry registry = DbValidationRuleRegistry.Create(builder =>
            builder.AddRule(
                "Slow",
                new DbValidationRuleOptions(Timeout: TimeSpan.FromMilliseconds(10)),
                static async (_, ct) =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), ct);
                    return DbValidationRuleResult.Success();
                }));
        var service = new DefaultValidationInferenceService(registry, DbExtensionPolicies.DefaultHostCallbackPolicy);
        FormDefinition form = CreateForm(new ControlDefinition(
            "c1",
            "text",
            new Rect(0, 0, 100, 30),
            new BindingDefinition("Name", "TwoWay"),
            PropertyBag.Empty,
            new ValidationOverride(false, [new ValidationRule("Slow", "Fallback", new Dictionary<string, object?>())], [])));

        IReadOnlyList<ValidationError> errors = await service.EvaluateAsync(
            form,
            new Dictionary<string, object?> { ["Name"] = "Alice" },
            TestContext.Current.CancellationToken);

        ValidationError error = Assert.Single(errors);
        Assert.Equal("Name", error.FieldName);
        Assert.Contains("timed out", error.Message);
    }

    private static FormDefinition CreateForm(params ControlDefinition[] controls)
        => new(
            "form-1",
            "Test Form",
            "Customers",
            1,
            "sig:customers",
            new LayoutDefinition("absolute", 8, true, [new Breakpoint("md", 0, null)]),
            controls);
}
