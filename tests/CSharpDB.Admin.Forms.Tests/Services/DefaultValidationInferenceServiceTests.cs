using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;

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
