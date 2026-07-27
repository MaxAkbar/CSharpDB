using CSharpDB.Migration;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationRejectSourceCapabilityValidatorTests
{
    private const string RuleId = "MIG-TEST-001";

    [Fact]
    public async Task ValidateForExecution_AcceptsExactCapabilityWithoutReadingSourceRows()
    {
        MigrationPlan plan = await CreatePlanAsync(deterministicRejects: true);
        var source = new CapabilityDataSource(
            plan.Source,
            MigrationRejectContract.DeterministicRejectsV1,
            new HashSet<string>([RuleId], StringComparer.Ordinal));

        MigrationRejectSourceCapabilityValidator.ValidateForExecution(plan, source);

        Assert.False(source.WasRead);
    }

    [Fact]
    public async Task ValidateForExecution_FailsClosedForMissingContractOrRule()
    {
        MigrationPlan plan = await CreatePlanAsync(deterministicRejects: true);
        var wrongContract = new CapabilityDataSource(
            plan.Source,
            "unsupported/v1",
            new HashSet<string>([RuleId], StringComparer.Ordinal));
        var missingRule = new CapabilityDataSource(
            plan.Source,
            MigrationRejectContract.DeterministicRejectsV1,
            new HashSet<string>(StringComparer.Ordinal));

        MigrationExecutionPolicyException contractError =
            Assert.Throws<MigrationExecutionPolicyException>(() =>
                MigrationRejectSourceCapabilityValidator.ValidateForExecution(
                    plan,
                    wrongContract));
        MigrationExecutionPolicyException ruleError =
            Assert.Throws<MigrationExecutionPolicyException>(() =>
                MigrationRejectSourceCapabilityValidator.ValidateForExecution(
                    plan,
                    missingRule));

        Assert.Equal("MIG-APPLY-POLICY-REJECT-SOURCE-001", contractError.Code);
        Assert.Equal("MIG-APPLY-POLICY-REJECT-RULE-001", ruleError.Code);
        Assert.False(wrongContract.WasRead);
        Assert.False(missingRule.WasRead);
    }

    [Fact]
    public async Task ValidateForExecution_PreservesFailFastCompatibilityForCapabilityBlindSources()
    {
        MigrationPlan plan = await CreatePlanAsync(deterministicRejects: false);
        var source = new CapabilityBlindDataSource(plan.Source);

        MigrationRejectSourceCapabilityValidator.ValidateForExecution(plan, source);

        Assert.False(source.WasRead);
    }

    private static async Task<MigrationPlan> CreatePlanAsync(bool deterministicRejects)
    {
        MigrationCatalog catalog = await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            },
            TestContext.Current.CancellationToken);
        var options = new MigrationPlanningOptions();
        if (deterministicRejects)
        {
            options = options with
            {
                Load = new MigrationLoadPolicy
                {
                    BatchSize = 10,
                    RejectMode = MigrationRejectMode.DeterministicRejects,
                    RejectPolicy = new MigrationDeterministicRejectPolicy
                    {
                        ContractVersion = MigrationRejectContract.DeterministicRejectsV1,
                        AllowedRuleIds = [RuleId],
                        MaxRejectedRowsPerBatch = 2,
                        MaxRejectedRowsPerRun = 10,
                        MaxRawValueBytes = 1_024,
                        MaxRawValueBytesPerBatch = 4_096,
                        MaxRawValueBytesPerRun = 8_192,
                        MaxArtifactBytes = 131_072,
                    },
                },
            };
        }

        return new MigrationPlanner().CreatePlan(catalog, options);
    }

    private class CapabilityBlindDataSource(MigrationSourceIdentity source) : IMigrationDataSource
    {
        public bool WasRead { get; private set; }

        public MigrationSourceIdentity Source { get; } = source;

        public string SnapshotIdentity => "snapshot:capability-preflight";

        public IAsyncEnumerable<MigrationDataBatch> ReadAsync(
            MigrationReadRequest request,
            CancellationToken cancellationToken = default)
        {
            WasRead = true;
            throw new InvalidOperationException("Capability preflight cannot enumerate rows.");
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class CapabilityDataSource(
        MigrationSourceIdentity source,
        string rejectContractVersion,
        IReadOnlySet<string> supportedRejectRuleIds) :
        CapabilityBlindDataSource(source),
        IMigrationRejectAwareDataSource
    {
        public string RejectContractVersion { get; } = rejectContractVersion;

        public IReadOnlySet<string> SupportedRejectRuleIds { get; } =
            supportedRejectRuleIds;
    }
}
