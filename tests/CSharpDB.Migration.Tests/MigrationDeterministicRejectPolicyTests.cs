using CSharpDB.Migration;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationDeterministicRejectPolicyTests
{
    [Fact]
    public async Task FailFastPolicy_RemainsAbsentFromCanonicalPlanArtifact()
    {
        (MigrationCatalog catalog, MigrationPlan plan) = await CreateArtifactsAsync();

        string original = MigrationArtifactSerializer.SerializePlan(
            plan,
            catalog,
            writeIndented: false);
        string explicitNull = MigrationArtifactSerializer.SerializePlan(
            plan with { Load = plan.Load with { RejectPolicy = null } },
            catalog,
            writeIndented: false);
        MigrationPlan restored = MigrationArtifactSerializer.DeserializePlan(original, catalog);

        Assert.Equal(original, explicitNull);
        Assert.DoesNotContain("\"rejectPolicy\"", original, StringComparison.Ordinal);
        Assert.Null(restored.Load.RejectPolicy);
        Assert.Equal(
            original,
            MigrationArtifactSerializer.SerializePlan(
                restored,
                catalog,
                writeIndented: false));
        Assert.Equal(
            MigrationArtifactSerializer.ComputePlanDigest(plan),
            MigrationArtifactSerializer.ComputePlanDigest(
                plan with { Load = plan.Load with { RejectPolicy = null } }));
        MigrationStagedTargetPolicyValidator.ValidateForBinding(plan);
        MigrationApplyPolicyValidator.ValidateForExecution(plan);
    }

    [Fact]
    public async Task DeterministicPolicy_RoundTripsWithCanonicalRuleOrderAndPlanBinding()
    {
        (MigrationCatalog catalog, MigrationPlan plan) = await CreateArtifactsAsync();
        MigrationPlan first = WithDeterministicPolicy(
            plan,
            ValidPolicy() with
            {
                AllowedRuleIds = ["MIG-CSV-ZZZ-001", "MIG-CSV-AAA-001"],
            });
        MigrationPlan reordered = WithDeterministicPolicy(
            plan,
            first.Load.RejectPolicy! with
            {
                AllowedRuleIds = first.Load.RejectPolicy!.AllowedRuleIds.Reverse().ToArray(),
            });

        string firstJson = MigrationArtifactSerializer.SerializePlan(
            first,
            catalog,
            writeIndented: false);
        string reorderedJson = MigrationArtifactSerializer.SerializePlan(
            reordered,
            catalog,
            writeIndented: false);
        MigrationPlan restored = MigrationArtifactSerializer.DeserializePlan(firstJson, catalog);

        Assert.Equal(firstJson, reorderedJson);
        Assert.Equal(
            ["MIG-CSV-AAA-001", "MIG-CSV-ZZZ-001"],
            restored.Load.RejectPolicy!.AllowedRuleIds);
        Assert.NotEqual(
            MigrationArtifactSerializer.ComputePlanDigest(plan),
            MigrationArtifactSerializer.ComputePlanDigest(restored));
        MigrationStagedTargetPolicyValidator.ValidateForBinding(restored);

        MigrationExecutionPolicyException error = Assert.Throws<MigrationExecutionPolicyException>(
            () => MigrationApplyPolicyValidator.ValidateForExecution(restored));
        Assert.Equal("MIG-APPLY-POLICY-REJECT-001", error.Code);
    }

    [Fact]
    public async Task RejectMode_RequiresItsMatchingNullablePolicy()
    {
        (MigrationCatalog catalog, MigrationPlan plan) = await CreateArtifactsAsync();
        MigrationPlan missing = plan with
        {
            Load = plan.Load with
            {
                RejectMode = MigrationRejectMode.DeterministicRejects,
                RejectPolicy = null,
            },
        };
        MigrationPlan unexpected = plan with
        {
            Load = plan.Load with { RejectPolicy = ValidPolicy() },
        };

        InvalidDataException missingError = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(missing, catalog));
        InvalidDataException unexpectedError = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(unexpected, catalog));

        Assert.Contains("requires", missingError.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("fail-fast", unexpectedError.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Throws<InvalidDataException>(
            () => MigrationStagedTargetPolicyValidator.ValidateForBinding(missing));
        Assert.Throws<InvalidDataException>(
            () => MigrationStagedTargetPolicyValidator.ValidateForBinding(unexpected));
    }

    [Fact]
    public async Task DeterministicPolicy_RejectsUnknownContractAndInvalidRuleRegistries()
    {
        (MigrationCatalog catalog, MigrationPlan plan) = await CreateArtifactsAsync();
        MigrationDeterministicRejectPolicy valid = ValidPolicy();
        MigrationDeterministicRejectPolicy[] invalidPolicies =
        [
            valid with { ContractVersion = "unknown/v1" },
            valid with { AllowedRuleIds = [] },
            valid with { AllowedRuleIds = null! },
            valid with { AllowedRuleIds = ["MIG-CSV-ROW-001", "MIG-CSV-ROW-001"] },
            valid with { AllowedRuleIds = ["mig-csv-row-001"] },
            valid with
            {
                AllowedRuleIds = Enumerable.Range(
                        0,
                        MigrationRejectContract.MaximumAllowedRuleIds + 1)
                    .Select(index => $"MIG-TEST-{index:D5}")
                    .ToArray(),
            },
        ];

        foreach (MigrationDeterministicRejectPolicy invalid in invalidPolicies)
        {
            Assert.Throws<InvalidDataException>(() =>
                MigrationArtifactSerializer.SerializePlan(
                    WithDeterministicPolicy(plan, invalid),
                    catalog));
        }
    }

    [Fact]
    public async Task DeterministicPolicy_EnforcesEveryPositiveAbsoluteCeiling()
    {
        (MigrationCatalog catalog, MigrationPlan plan) = await CreateArtifactsAsync();
        MigrationDeterministicRejectPolicy valid = ValidPolicy();
        MigrationDeterministicRejectPolicy[] invalidPolicies =
        [
            valid with { MaxRejectedRowsPerBatch = 0 },
            valid with
            {
                MaxRejectedRowsPerBatch =
                    MigrationRejectContract.MaximumRejectedRowsPerBatch + 1,
            },
            valid with { MaxRejectedRowsPerRun = 0 },
            valid with
            {
                MaxRejectedRowsPerRun =
                    MigrationRejectContract.MaximumRejectedRowsPerRun + 1,
            },
            valid with { MaxRawValueBytes = 0 },
            valid with
            {
                MaxRawValueBytes = MigrationRejectContract.MaximumRawValueBytes + 1,
            },
            valid with { MaxRawValueBytesPerBatch = 0 },
            valid with
            {
                MaxRawValueBytesPerBatch =
                    MigrationRejectContract.MaximumRawValueBytesPerBatch + 1,
            },
            valid with { MaxRawValueBytesPerRun = 0 },
            valid with
            {
                MaxRawValueBytesPerRun =
                    MigrationRejectContract.MaximumRawValueBytesPerRun + 1,
            },
            valid with { MaxArtifactBytes = 0 },
            valid with
            {
                MaxArtifactBytes = MigrationRejectContract.MaximumArtifactBytes + 1,
            },
        ];

        foreach (MigrationDeterministicRejectPolicy invalid in invalidPolicies)
        {
            Assert.Throws<InvalidDataException>(() =>
                MigrationArtifactSerializer.SerializePlan(
                    WithDeterministicPolicy(plan, invalid),
                    catalog));
        }
    }

    [Fact]
    public async Task DeterministicPolicy_EnforcesBatchAndRunLimitOrdering()
    {
        (MigrationCatalog catalog, MigrationPlan plan) = await CreateArtifactsAsync();
        MigrationDeterministicRejectPolicy valid = ValidPolicy();
        MigrationDeterministicRejectPolicy[] invalidPolicies =
        [
            valid with { MaxRejectedRowsPerBatch = plan.Load.BatchSize + 1 },
            valid with
            {
                MaxRejectedRowsPerBatch = 11,
                MaxRejectedRowsPerRun = 10,
            },
            valid with
            {
                MaxRawValueBytes = 1_025,
                MaxRawValueBytesPerBatch = 1_024,
            },
            valid with
            {
                MaxRawValueBytesPerBatch = 8_193,
                MaxRawValueBytesPerRun = 8_192,
            },
        ];

        foreach (MigrationDeterministicRejectPolicy invalid in invalidPolicies)
        {
            Assert.Throws<InvalidDataException>(() =>
                MigrationArtifactSerializer.SerializePlan(
                    WithDeterministicPolicy(plan, invalid),
                    catalog));
        }
    }

    [Fact]
    public async Task DeterministicTargetBinding_RejectsOversizedIncludedObjectIds()
    {
        (_, MigrationPlan plan) = await CreateArtifactsAsync();
        MigrationPlanObject included = plan.Objects.First(item => item.Included);
        string oversizedObjectId = new(
            'x',
            MigrationRejectContract.MaximumObjectIdCharacters + 1);
        MigrationPlan deterministic = WithDeterministicPolicy(
            plan with
            {
                Objects = plan.Objects.Select(item =>
                        string.Equals(
                            item.SourceObjectId,
                            included.SourceObjectId,
                            StringComparison.Ordinal)
                            ? item with { SourceObjectId = oversizedObjectId }
                            : item)
                    .ToArray(),
            },
            ValidPolicy());

        InvalidDataException error = Assert.Throws<InvalidDataException>(() =>
            MigrationStagedTargetPolicyValidator.ValidateForBinding(deterministic));

        Assert.Contains("bounded", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("object", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DeterministicPolicy_ArtifactLimitMustFitCanonicalHeader()
    {
        (_, MigrationPlan plan) = await CreateArtifactsAsync();
        MigrationDeterministicRejectPolicy valid = ValidPolicy();
        MigrationPlan tooSmall = WithDeterministicPolicy(
            plan,
            valid with
            {
                MaxArtifactBytes = MigrationRejectLedgerCodec.MinimumCanonicalArtifactBytes - 1,
            });
        MigrationPlan exactMinimum = WithDeterministicPolicy(
            plan,
            valid with
            {
                MaxArtifactBytes = MigrationRejectLedgerCodec.MinimumCanonicalArtifactBytes,
            });

        InvalidDataException error = Assert.Throws<InvalidDataException>(() =>
            MigrationStagedTargetPolicyValidator.ValidateForBinding(tooSmall));

        Assert.Contains("header", error.Message, StringComparison.OrdinalIgnoreCase);
        MigrationStagedTargetPolicyValidator.ValidateForBinding(exactMinimum);
    }

    private static MigrationPlan WithDeterministicPolicy(
        MigrationPlan plan,
        MigrationDeterministicRejectPolicy policy) =>
        plan with
        {
            Load = plan.Load with
            {
                RejectMode = MigrationRejectMode.DeterministicRejects,
                RejectPolicy = policy,
            },
        };

    private static MigrationDeterministicRejectPolicy ValidPolicy() => new()
    {
        ContractVersion = MigrationRejectContract.DeterministicRejectsV1,
        AllowedRuleIds = ["MIG-CSV-ROW-001"],
        MaxRejectedRowsPerBatch = 10,
        MaxRejectedRowsPerRun = 100,
        MaxRawValueBytes = 1_024,
        MaxRawValueBytesPerBatch = 8_192,
        MaxRawValueBytesPerRun = 65_536,
        MaxArtifactBytes = 131_072,
    };

    private static async Task<(MigrationCatalog Catalog, MigrationPlan Plan)> CreateArtifactsAsync()
    {
        MigrationCatalog catalog = await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            },
            TestContext.Current.CancellationToken);
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        return (catalog, plan);
    }
}
