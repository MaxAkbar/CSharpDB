using CSharpDB.Admin.Services;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Tests.Admin;

public sealed class HostCallbackPolicyServiceTests
{
    [Fact]
    public void Evaluate_DefaultPolicyAllowsAdminHostCallbacks()
    {
        var service = new HostCallbackPolicyService(AdminHostCallbacks.CreatePolicy());
        DbHostCallbackDescriptor[] callbacks =
        [
            .. AdminHostCallbacks.CreateFunctionRegistry().Callbacks,
            .. AdminHostCallbacks.CreateCommandRegistry().Callbacks,
            .. CreateValidationRuleRegistry().Callbacks,
        ];

        Assert.NotEmpty(callbacks);
        Assert.All(callbacks, callback =>
        {
            DbExtensionPolicyDecision decision = service.Evaluate(callback);

            Assert.True(decision.Allowed, decision.DenialReason);
            Assert.Null(decision.DenialReason);
            Assert.All(decision.Capabilities, capability =>
                Assert.Equal(DbExtensionCapabilityGrantStatus.Granted, capability.Status));
        });
    }

    [Fact]
    public void Evaluate_DefaultPolicyDeniesUnapprovedAdditionalCapabilities()
    {
        DbCommandRegistry commands = DbCommandRegistry.Create(builder =>
            builder.AddCommand(
                "NotifyExternalSystem",
                new DbCommandOptions(
                    AdditionalCapabilities:
                    [
                        new DbExtensionCapabilityRequest(DbExtensionCapability.Network),
                    ]),
                static _ => DbCommandResult.Success()));
        DbHostCallbackDescriptor callback = Assert.Single(commands.Callbacks);
        var service = new HostCallbackPolicyService(AdminHostCallbacks.CreatePolicy());

        DbExtensionPolicyDecision decision = service.Evaluate(callback);

        Assert.False(decision.Allowed);
        Assert.Equal("No grant exists for capability 'Network'.", decision.DenialReason);
        Assert.Contains(decision.Capabilities, capability =>
            capability.Name == DbExtensionCapability.Commands
            && capability.Status == DbExtensionCapabilityGrantStatus.Granted);
        Assert.Contains(decision.Capabilities, capability =>
            capability.Name == DbExtensionCapability.Network
            && capability.Status == DbExtensionCapabilityGrantStatus.Denied);
    }

    [Fact]
    public void Evaluate_UsesCallbackTimeoutBeforeDefaultPolicyTimeout()
    {
        DbCommandRegistry commands = DbCommandRegistry.Create(builder =>
            builder.AddCommand(
                "LongCommand",
                new DbCommandOptions(Timeout: TimeSpan.FromSeconds(17)),
                static _ => DbCommandResult.Success()));
        DbHostCallbackDescriptor callback = Assert.Single(commands.Callbacks);
        var service = new HostCallbackPolicyService(AdminHostCallbacks.CreatePolicy());

        DbExtensionPolicyDecision decision = service.Evaluate(callback);

        Assert.True(decision.Allowed);
        Assert.Equal(TimeSpan.FromSeconds(17), decision.Timeout);
    }

    private static DbValidationRuleRegistry CreateValidationRuleRegistry()
        => DbValidationRuleRegistry.Create(builder =>
        {
            builder.AddRule(
                "AdminHostValidationRule",
                new DbValidationRuleOptions(Description: "Test validation rule for Admin host policy."),
                static (_, _) => ValueTask.FromResult(DbValidationRuleResult.Success()));
        });
}
