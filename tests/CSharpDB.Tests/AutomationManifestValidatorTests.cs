using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class AutomationManifestValidatorTests
{
    [Fact]
    public void Validate_ReturnsSuccessWhenMetadataMatchesRegistries()
    {
        DbAutomationMetadata metadata = new(
            Commands:
            [
                new DbAutomationCommandReference("AuditOrder", "admin.forms", "form.events.BeforeInsert"),
            ],
            ScalarFunctions:
            [
                new DbAutomationScalarFunctionReference("Slugify", 1, "admin.forms", "controls.slug.formula"),
            ],
            ValidationRules:
            [
                new DbAutomationValidationRuleReference("CreditLimit", "admin.forms", "controls.credit.validationRules.CreditLimit"),
            ]);
        DbFunctionRegistry functions = CreateFunctions(("Slugify", 1));
        DbCommandRegistry commands = CreateCommands("AuditOrder");
        DbValidationRuleRegistry validationRules = CreateValidationRules("CreditLimit");

        AutomationValidationResult result = AutomationManifestValidator.Validate(metadata, functions, commands, validationRules);

        Assert.True(result.Succeeded);
        Assert.Empty(result.Issues);
    }

    [Fact]
    public void Validate_ReportsMissingCommandsAndScalarFunctions()
    {
        DbAutomationMetadata metadata = new(
            Commands:
            [
                new DbAutomationCommandReference("AuditOrder", "admin.forms", "form.events.BeforeInsert"),
            ],
            ScalarFunctions:
            [
                new DbAutomationScalarFunctionReference("Slugify", 1, "admin.forms", "controls.slug.formula"),
            ],
            ValidationRules:
            [
                new DbAutomationValidationRuleReference("CreditLimit", "admin.forms", "form.validationRules.CreditLimit"),
            ]);

        AutomationValidationResult result = AutomationManifestValidator.Validate(
            metadata,
            DbFunctionRegistry.Empty,
            DbCommandRegistry.Empty,
            DbValidationRuleRegistry.Empty);

        Assert.False(result.Succeeded);
        Assert.Equal(3, result.Issues.Count);

        AutomationValidationIssue commandIssue = Assert.Single(
            result.Issues,
            issue => issue.CallbackKind == AutomationCallbackKind.Command);
        Assert.Equal(AutomationValidationSeverity.Error, commandIssue.Severity);
        Assert.Equal("AuditOrder", commandIssue.Name);
        Assert.Equal("admin.forms", commandIssue.Surface);
        Assert.Equal("form.events.BeforeInsert", commandIssue.Location);
        Assert.Contains("not registered", commandIssue.Message);

        AutomationValidationIssue functionIssue = Assert.Single(
            result.Issues,
            issue => issue.CallbackKind == AutomationCallbackKind.ScalarFunction);
        Assert.Equal(AutomationValidationSeverity.Error, functionIssue.Severity);
        Assert.Equal("Slugify", functionIssue.Name);
        Assert.Equal(1, functionIssue.ExpectedArity);
        Assert.Equal("controls.slug.formula", functionIssue.Location);
        Assert.Contains("not registered", functionIssue.Message);

        AutomationValidationIssue validationIssue = Assert.Single(
            result.Issues,
            issue => issue.CallbackKind == AutomationCallbackKind.ValidationRule);
        Assert.Equal(AutomationValidationSeverity.Error, validationIssue.Severity);
        Assert.Equal("CreditLimit", validationIssue.Name);
        Assert.Equal("form.validationRules.CreditLimit", validationIssue.Location);
        Assert.Contains("not registered", validationIssue.Message);
    }

    [Fact]
    public void Validate_ReportsScalarFunctionArityMismatch()
    {
        DbAutomationMetadata metadata = new(
            ScalarFunctions:
            [
                new DbAutomationScalarFunctionReference("Slugify", 2, "pipelines", "transforms[0].derivedColumns[0].expression"),
            ]);
        DbFunctionRegistry functions = CreateFunctions(("Slugify", 1));

        AutomationValidationResult result = AutomationManifestValidator.Validate(
            metadata,
            functions,
            DbCommandRegistry.Empty);

        AutomationValidationIssue issue = Assert.Single(result.Issues);
        Assert.False(result.Succeeded);
        Assert.Equal(AutomationValidationSeverity.Error, issue.Severity);
        Assert.Equal(AutomationCallbackKind.ScalarFunction, issue.CallbackKind);
        Assert.Equal("Slugify", issue.Name);
        Assert.Equal(2, issue.ExpectedArity);
        Assert.Contains("with arity 2", issue.Message);
        Assert.Contains("host registry has arity 1", issue.Message);
    }

    [Fact]
    public void Validate_DuplicateReferencesProduceWarningsWithoutFailing()
    {
        DbAutomationMetadata metadata = new(
            Commands:
            [
                new DbAutomationCommandReference("AuditOrder", "admin.forms", "form.events.BeforeInsert"),
                new DbAutomationCommandReference("auditorder", "admin.forms", "form.events.BeforeInsert"),
            ],
            ScalarFunctions:
            [
                new DbAutomationScalarFunctionReference("Slugify", 1, "admin.forms", "controls.slug.formula"),
                new DbAutomationScalarFunctionReference("slugify", 1, "admin.forms", "controls.slug.formula"),
            ],
            ValidationRules:
            [
                new DbAutomationValidationRuleReference("CreditLimit", "admin.forms", "form.validationRules.CreditLimit"),
                new DbAutomationValidationRuleReference("creditlimit", "admin.forms", "form.validationRules.CreditLimit"),
            ]);
        DbFunctionRegistry functions = CreateFunctions(("Slugify", 1));
        DbCommandRegistry commands = CreateCommands("AuditOrder");
        DbValidationRuleRegistry validationRules = CreateValidationRules("CreditLimit");

        AutomationValidationResult result = AutomationManifestValidator.Validate(metadata, functions, commands, validationRules);

        Assert.True(result.Succeeded);
        Assert.Equal(3, result.Issues.Count);
        Assert.All(result.Issues, issue => Assert.Equal(AutomationValidationSeverity.Warning, issue.Severity));
        Assert.Contains(result.Issues, issue => issue.CallbackKind == AutomationCallbackKind.Command);
        Assert.Contains(result.Issues, issue => issue.CallbackKind == AutomationCallbackKind.ScalarFunction);
        Assert.Contains(result.Issues, issue => issue.CallbackKind == AutomationCallbackKind.ValidationRule);
    }

    [Fact]
    public void Validate_CanRequireAutomationMetadata()
    {
        AutomationValidationResult optionalResult = AutomationManifestValidator.Validate(
            metadata: null,
            DbFunctionRegistry.Empty,
            DbCommandRegistry.Empty);
        AutomationValidationResult requiredResult = AutomationManifestValidator.Validate(
            metadata: null,
            DbFunctionRegistry.Empty,
            DbCommandRegistry.Empty,
            new AutomationManifestValidationOptions(RequireMetadata: true));

        Assert.True(optionalResult.Succeeded);
        Assert.Empty(optionalResult.Issues);

        AutomationValidationIssue issue = Assert.Single(requiredResult.Issues);
        Assert.False(requiredResult.Succeeded);
        Assert.Equal(AutomationCallbackKind.Unknown, issue.CallbackKind);
        Assert.Equal(AutomationValidationSeverity.Error, issue.Severity);
        Assert.Contains("metadata is missing", issue.Message);
    }

    private static DbFunctionRegistry CreateFunctions(params (string Name, int Arity)[] functions)
        => DbFunctionRegistry.Create(builder =>
        {
            foreach ((string name, int arity) in functions)
                builder.AddScalar(name, arity, static (_, _) => DbValue.Null);
        });

    private static DbCommandRegistry CreateCommands(params string[] commands)
        => DbCommandRegistry.Create(builder =>
        {
            foreach (string command in commands)
                builder.AddCommand(command, static _ => DbCommandResult.Success());
        });

    private static DbValidationRuleRegistry CreateValidationRules(params string[] rules)
        => DbValidationRuleRegistry.Create(builder =>
        {
            foreach (string rule in rules)
                builder.AddRule(rule, static (_, _) => ValueTask.FromResult(DbValidationRuleResult.Success()));
        });
}
