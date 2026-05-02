using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class DbValidationRuleRegistryTests
{
    [Fact]
    public void AddRule_RegistersDescriptorWithValidationCapability()
    {
        DbValidationRuleRegistry registry = DbValidationRuleRegistry.Create(builder =>
            builder.AddRule(
                "CreditLimit",
                new DbValidationRuleOptions(
                    Description: "Rejects orders over the customer credit limit.",
                    Timeout: TimeSpan.FromSeconds(2)),
                static (_, _) => ValueTask.FromResult(DbValidationRuleResult.Success())));

        Assert.True(registry.TryGetRule("creditlimit", out DbValidationRuleDefinition definition));
        Assert.Equal("CreditLimit", definition.Name);
        Assert.Equal(AutomationCallbackKind.ValidationRule, definition.Descriptor.Kind);
        Assert.Equal(TimeSpan.FromSeconds(2), definition.Descriptor.Timeout);
        Assert.Contains(definition.Descriptor.Capabilities, capability =>
            capability.Name == DbExtensionCapability.ValidationRules
            && capability.Exports is not null
            && capability.Exports.Contains("CreditLimit"));
        Assert.Same(definition.Descriptor, Assert.Single(registry.Callbacks));
    }

    [Fact]
    public void AddRule_RejectsDuplicateNames()
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            DbValidationRuleRegistry.Create(builder =>
            {
                builder.AddRule("CreditLimit", static (_, _) => ValueTask.FromResult(DbValidationRuleResult.Success()));
                builder.AddRule("creditlimit", static (_, _) => ValueTask.FromResult(DbValidationRuleResult.Success()));
            }));

        Assert.Contains("already registered", ex.Message);
    }

    [Fact]
    public void AddRule_RejectsInvalidNamesAndTimeouts()
    {
        Assert.Throws<ArgumentException>(() =>
            DbValidationRuleRegistry.Create(builder =>
                builder.AddRule("not valid", static (_, _) => ValueTask.FromResult(DbValidationRuleResult.Success()))));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            DbValidationRuleRegistry.Create(builder =>
                builder.AddRule(
                    "CreditLimit",
                    new DbValidationRuleOptions(Timeout: TimeSpan.Zero),
                    static (_, _) => ValueTask.FromResult(DbValidationRuleResult.Success()))));
    }

    [Fact]
    public void Policy_DefaultHostCallbackPolicyAllowsValidationRules()
    {
        DbValidationRuleRegistry registry = DbValidationRuleRegistry.Create(builder =>
            builder.AddRule("CreditLimit", static (_, _) => ValueTask.FromResult(DbValidationRuleResult.Success())));
        DbValidationRuleDefinition definition = Assert.Single(registry.Rules);

        DbExtensionPolicyDecision decision = DbExtensionPolicyEvaluator.Evaluate(
            definition.Descriptor,
            DbExtensionPolicies.DefaultHostCallbackPolicy,
            DbExtensionHostMode.Embedded);

        Assert.True(decision.Allowed, decision.DenialReason);
        Assert.Contains(decision.Capabilities, capability =>
            capability.Name == DbExtensionCapability.ValidationRules
            && capability.Status == DbExtensionCapabilityGrantStatus.Granted);
    }

    [Fact]
    public void Policy_ScopedExportDenyBlocksOnlyMatchingRule()
    {
        DbValidationRuleRegistry registry = DbValidationRuleRegistry.Create(builder =>
            builder.AddRule("CreditLimit", static (_, _) => ValueTask.FromResult(DbValidationRuleResult.Success())));
        DbValidationRuleDefinition definition = Assert.Single(registry.Rules);
        var policy = new DbExtensionPolicy(
            AllowExtensions: true,
            Grants:
            [
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.ValidationRules,
                    DbExtensionCapabilityGrantStatus.Granted,
                    Exports: ["*"]),
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.ValidationRules,
                    DbExtensionCapabilityGrantStatus.Denied,
                    Reason: "Credit limit validation is disabled.",
                    Exports: ["CreditLimit"]),
            ]);

        DbExtensionPolicyDecision decision = DbExtensionPolicyEvaluator.Evaluate(
            definition.Descriptor,
            policy,
            DbExtensionHostMode.Embedded);

        Assert.False(decision.Allowed);
        Assert.Equal("Credit limit validation is disabled.", decision.DenialReason);
    }
}
