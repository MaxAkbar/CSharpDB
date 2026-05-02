using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class DbExtensionPolicyTests
{
    [Fact]
    public void Evaluate_DeniesExtensionsWhenHostPolicyDisablesExecution()
    {
        DbExtensionManifest manifest = CreateCommandManifest(signature: "trusted-signature");
        var policy = new DbExtensionPolicy(
            AllowExtensions: false,
            Grants:
            [
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.Commands,
                    DbExtensionCapabilityGrantStatus.Granted),
            ],
            RequireSignature: true);

        DbExtensionPolicyDecision decision = DbExtensionPolicyEvaluator.Evaluate(
            manifest,
            policy,
            DbExtensionHostMode.Embedded);

        Assert.False(decision.Allowed);
        Assert.Equal("Extension execution is disabled by host policy.", decision.DenialReason);
    }

    [Fact]
    public void Evaluate_DeniesUnsignedExtensionWhenSignatureIsRequired()
    {
        DbExtensionManifest manifest = CreateCommandManifest(signature: null);
        var policy = new DbExtensionPolicy(
            AllowExtensions: true,
            Grants:
            [
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.Commands,
                    DbExtensionCapabilityGrantStatus.Granted),
            ],
            RequireSignature: true);

        DbExtensionPolicyDecision decision = DbExtensionPolicyEvaluator.Evaluate(
            manifest,
            policy,
            DbExtensionHostMode.Embedded);

        Assert.False(decision.Allowed);
        Assert.Equal("Extension policy requires a signature.", decision.DenialReason);
    }

    [Fact]
    public void Evaluate_AllowsHostCallbackWithoutArtifactSignature()
    {
        DbExtensionManifest manifest = CreateCommandManifest(signature: null) with
        {
            Runtime = DbExtensionRuntimeKind.HostCallback,
            Entrypoint = "host:ApproveOrder",
            ArtifactSha256 = null,
        };
        var policy = new DbExtensionPolicy(
            AllowExtensions: true,
            Grants:
            [
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.Commands,
                    DbExtensionCapabilityGrantStatus.Granted),
            ],
            RequireSignature: true);

        DbExtensionPolicyDecision decision = DbExtensionPolicyEvaluator.Evaluate(
            manifest,
            policy,
            DbExtensionHostMode.Embedded);

        Assert.True(decision.Allowed);
        Assert.Null(decision.DenialReason);
    }

    [Fact]
    public void Evaluate_AllowsHostCallbackDescriptorWhenCapabilitiesAreGranted()
    {
        DbCommandRegistry registry = DbCommandRegistry.Create(commands =>
            commands.AddCommand(
                "ApproveOrder",
                new DbCommandOptions(
                    Timeout: TimeSpan.FromSeconds(2),
                    AdditionalCapabilities:
                    [
                        new DbExtensionCapabilityRequest(
                            DbExtensionCapability.ReadDatabase,
                            Tables: ["Orders"]),
                    ]),
                static _ => DbCommandResult.Success()));
        DbHostCallbackDescriptor descriptor = Assert.Single(registry.Callbacks);
        var policy = new DbExtensionPolicy(
            AllowExtensions: true,
            Grants:
            [
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.Commands,
                    DbExtensionCapabilityGrantStatus.Granted),
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.ReadDatabase,
                    DbExtensionCapabilityGrantStatus.Granted,
                    PolicySource: "test-policy"),
            ],
            DefaultTimeout: TimeSpan.FromSeconds(10),
            RequireSignature: true);

        DbExtensionPolicyDecision decision = DbExtensionPolicyEvaluator.Evaluate(
            descriptor,
            policy,
            DbExtensionHostMode.Embedded);

        Assert.True(decision.Allowed);
        Assert.Null(decision.DenialReason);
        Assert.Equal(TimeSpan.FromSeconds(2), decision.Timeout);
        Assert.Equal(
            [DbExtensionCapability.Commands, DbExtensionCapability.ReadDatabase],
            decision.Capabilities.Select(static capability => capability.Name).ToArray());
    }

    [Fact]
    public void Evaluate_DeniesHostCallbackDescriptorWhenCapabilityIsNotGranted()
    {
        DbCommandRegistry registry = DbCommandRegistry.Create(commands =>
            commands.AddCommand(
                "NotifyExternalSystem",
                new DbCommandOptions(
                    AdditionalCapabilities:
                    [
                        new DbExtensionCapabilityRequest(DbExtensionCapability.Network),
                    ]),
                static _ => DbCommandResult.Success()));
        DbHostCallbackDescriptor descriptor = Assert.Single(registry.Callbacks);
        var policy = new DbExtensionPolicy(
            AllowExtensions: true,
            Grants:
            [
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.Commands,
                    DbExtensionCapabilityGrantStatus.Granted),
            ],
            RequireSignature: true);

        DbExtensionPolicyDecision decision = DbExtensionPolicyEvaluator.Evaluate(
            descriptor,
            policy,
            DbExtensionHostMode.Embedded);

        Assert.False(decision.Allowed);
        Assert.Equal("No grant exists for capability 'Network'.", decision.DenialReason);
    }

    [Fact]
    public void Evaluate_AllowsSignedExtensionWhenRequestedCapabilitiesAreGranted()
    {
        DbExtensionManifest manifest = CreateCommandManifest(signature: "trusted-signature");
        var policy = new DbExtensionPolicy(
            AllowExtensions: true,
            Grants:
            [
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.Commands,
                    DbExtensionCapabilityGrantStatus.Granted,
                    Reason: "Approved test command.",
                    PolicySource: "unit-test"),
            ],
            DefaultTimeout: TimeSpan.FromSeconds(3),
            MaxMemoryBytes: 64 * 1024 * 1024,
            RequireSignature: true);

        DbExtensionPolicyDecision decision = DbExtensionPolicyEvaluator.Evaluate(
            manifest,
            policy,
            DbExtensionHostMode.Embedded);

        Assert.True(decision.Allowed);
        Assert.Null(decision.DenialReason);
        DbExtensionCapabilityDecision capability = Assert.Single(decision.Capabilities);
        Assert.Equal(DbExtensionCapability.Commands, capability.Name);
        Assert.Equal(DbExtensionCapabilityGrantStatus.Granted, capability.Status);
        Assert.Equal("unit-test", capability.PolicySource);
        Assert.Equal(TimeSpan.FromSeconds(3), decision.Timeout);
        Assert.Equal(64 * 1024 * 1024, decision.MaxMemoryBytes);
    }

    [Fact]
    public void Evaluate_DeniesExtensionWhenRequestedCapabilityIsMissing()
    {
        DbExtensionManifest manifest = CreateCommandManifest(signature: "trusted-signature") with
        {
            Capabilities =
            [
                new DbExtensionCapabilityRequest(DbExtensionCapability.Commands),
                new DbExtensionCapabilityRequest(DbExtensionCapability.ReadDatabase, Tables: ["Orders"]),
            ],
        };
        var policy = new DbExtensionPolicy(
            AllowExtensions: true,
            Grants:
            [
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.Commands,
                    DbExtensionCapabilityGrantStatus.Granted),
            ],
            RequireSignature: true);

        DbExtensionPolicyDecision decision = DbExtensionPolicyEvaluator.Evaluate(
            manifest,
            policy,
            DbExtensionHostMode.Embedded);

        Assert.False(decision.Allowed);
        Assert.Equal("No grant exists for capability 'ReadDatabase'.", decision.DenialReason);
    }

    [Fact]
    public void Evaluate_AllowsExtensionWhenScopedGrantMatchesRequestedTable()
    {
        DbExtensionManifest manifest = CreateCommandManifest(signature: "trusted-signature") with
        {
            Capabilities =
            [
                new DbExtensionCapabilityRequest(DbExtensionCapability.Commands),
                new DbExtensionCapabilityRequest(DbExtensionCapability.ReadDatabase, Tables: ["Orders"]),
            ],
        };
        var policy = new DbExtensionPolicy(
            AllowExtensions: true,
            Grants:
            [
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.Commands,
                    DbExtensionCapabilityGrantStatus.Granted),
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.ReadDatabase,
                    DbExtensionCapabilityGrantStatus.Granted,
                    Tables: ["Orders"]),
            ],
            RequireSignature: true);

        DbExtensionPolicyDecision decision = DbExtensionPolicyEvaluator.Evaluate(
            manifest,
            policy,
            DbExtensionHostMode.Embedded);

        Assert.True(decision.Allowed, decision.DenialReason);
        Assert.All(decision.Capabilities, capability =>
            Assert.Equal(DbExtensionCapabilityGrantStatus.Granted, capability.Status));
    }

    [Fact]
    public void Evaluate_DeniesExtensionWhenScopedGrantDoesNotMatchRequestedTable()
    {
        DbExtensionManifest manifest = CreateCommandManifest(signature: "trusted-signature") with
        {
            Capabilities =
            [
                new DbExtensionCapabilityRequest(DbExtensionCapability.Commands),
                new DbExtensionCapabilityRequest(DbExtensionCapability.ReadDatabase, Tables: ["Orders"]),
            ],
        };
        var policy = new DbExtensionPolicy(
            AllowExtensions: true,
            Grants:
            [
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.Commands,
                    DbExtensionCapabilityGrantStatus.Granted),
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.ReadDatabase,
                    DbExtensionCapabilityGrantStatus.Granted,
                    Tables: ["Customers"]),
            ],
            RequireSignature: true);

        DbExtensionPolicyDecision decision = DbExtensionPolicyEvaluator.Evaluate(
            manifest,
            policy,
            DbExtensionHostMode.Embedded);

        Assert.False(decision.Allowed);
        Assert.Equal(
            "No grant for capability 'ReadDatabase' matches requested exports [*], tables [Orders], scope [*].",
            decision.DenialReason);
    }

    [Fact]
    public void Evaluate_DenyGrantWinsOverMatchingAllowGrant()
    {
        DbExtensionManifest manifest = CreateCommandManifest(signature: "trusted-signature") with
        {
            Capabilities =
            [
                new DbExtensionCapabilityRequest(DbExtensionCapability.Commands),
                new DbExtensionCapabilityRequest(DbExtensionCapability.ReadDatabase, Tables: ["Orders"]),
            ],
        };
        var policy = new DbExtensionPolicy(
            AllowExtensions: true,
            Grants:
            [
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.Commands,
                    DbExtensionCapabilityGrantStatus.Granted),
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.ReadDatabase,
                    DbExtensionCapabilityGrantStatus.Granted,
                    Tables: ["Orders"]),
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.ReadDatabase,
                    DbExtensionCapabilityGrantStatus.Denied,
                    Reason: "Orders reads are blocked for this host.",
                    Tables: ["Orders"]),
            ],
            RequireSignature: true);

        DbExtensionPolicyDecision decision = DbExtensionPolicyEvaluator.Evaluate(
            manifest,
            policy,
            DbExtensionHostMode.Embedded);

        Assert.False(decision.Allowed);
        Assert.Equal("Orders reads are blocked for this host.", decision.DenialReason);
        DbExtensionCapabilityDecision readDecision = Assert.Single(
            decision.Capabilities,
            capability => capability.Name == DbExtensionCapability.ReadDatabase);
        Assert.Equal(DbExtensionCapabilityGrantStatus.Denied, readDecision.Status);
    }

    [Fact]
    public void Evaluate_ScopedDenyDoesNotBlockDifferentScope()
    {
        DbExtensionManifest manifest = CreateCommandManifest(signature: "trusted-signature") with
        {
            Capabilities =
            [
                new DbExtensionCapabilityRequest(DbExtensionCapability.Commands),
                new DbExtensionCapabilityRequest(DbExtensionCapability.ReadDatabase, Tables: ["Orders"]),
            ],
        };
        var policy = new DbExtensionPolicy(
            AllowExtensions: true,
            Grants:
            [
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.Commands,
                    DbExtensionCapabilityGrantStatus.Granted),
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.ReadDatabase,
                    DbExtensionCapabilityGrantStatus.Denied,
                    Tables: ["Payroll"]),
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.ReadDatabase,
                    DbExtensionCapabilityGrantStatus.Granted,
                    Tables: ["Orders"]),
            ],
            RequireSignature: true);

        DbExtensionPolicyDecision decision = DbExtensionPolicyEvaluator.Evaluate(
            manifest,
            policy,
            DbExtensionHostMode.Embedded);

        Assert.True(decision.Allowed, decision.DenialReason);
    }

    [Fact]
    public void Evaluate_DeniesExtensionWhenHostModeIsNotAllowed()
    {
        DbExtensionManifest manifest = CreateCommandManifest(signature: "trusted-signature");
        var policy = new DbExtensionPolicy(
            AllowExtensions: true,
            Grants:
            [
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.Commands,
                    DbExtensionCapabilityGrantStatus.Granted),
            ],
            RequireSignature: true,
            AllowedHostModes: DbExtensionHostMode.Daemon);

        DbExtensionPolicyDecision decision = DbExtensionPolicyEvaluator.Evaluate(
            manifest,
            policy,
            DbExtensionHostMode.Embedded);

        Assert.False(decision.Allowed);
        Assert.Equal("Extension execution is not allowed in Embedded mode.", decision.DenialReason);
    }

    private static DbExtensionManifest CreateCommandManifest(string? signature)
        => new(
            Id: "com.example.order-automation",
            Name: "Order Automation",
            Version: "1.2.0",
            Runtime: DbExtensionRuntimeKind.OutOfProcess,
            Entrypoint: "order-automation",
            Exports:
            [
                new DbExtensionExport(DbExtensionExportKind.Command, "ApproveOrder"),
            ],
            Capabilities:
            [
                new DbExtensionCapabilityRequest(
                    DbExtensionCapability.Commands,
                    Reason: "Approve orders from form automation.",
                    Exports: ["ApproveOrder"]),
            ],
            RequiredCSharpDbVersion: "9.0",
            ArtifactSha256: "abc123",
            Signature: signature,
            Publisher: "Example Co.");
}
