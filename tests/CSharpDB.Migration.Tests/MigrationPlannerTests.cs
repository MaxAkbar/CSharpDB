using System.Reflection;
using System.Text.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationPlannerTests
{
    [Fact]
    public void EmbeddedCapabilities_MatchTheCurrentPrimitiveSurface()
    {
        CSharpDbCapabilityCatalog capabilities = CSharpDbCapabilityCatalogLoader.LoadEmbedded();

        Assert.Equal("4.4.0", capabilities.TargetCSharpDbVersion);
        Assert.Equal("local-typed-engine", capabilities.Surface);
        Assert.Equal(SqlIdentifierRules.MaxLength, capabilities.MaxIdentifierLength);
        Assert.Equal(64, capabilities.Digest.Length);
        Assert.Equal(Enum.GetValues<DbType>(), capabilities.ValueTypes.Select(item => item.Type));
        Assert.False(capabilities.IsColumnType(DbType.Null));
        Assert.All(
            new[] { DbType.Integer, DbType.Real, DbType.Text, DbType.Blob },
            type => Assert.True(capabilities.IsColumnType(type)));
        Assert.Equal(
            MigrationCompatibilityStatus.Unsupported,
            capabilities.GetObjectStatus(MigrationObjectKind.Sequence));
        Assert.Equal(
            MigrationCompatibilityStatus.Conditional,
            capabilities.GetObjectStatus(MigrationObjectKind.Trigger));
        CSharpDbCapabilityRule indexRule = capabilities.Rules.Single(rule =>
            rule.ObjectKind == MigrationObjectKind.Index &&
            rule.Feature == CSharpDbCapabilityFeature.Index);
        Assert.Equal(
            [DbType.Integer, DbType.Real, DbType.Text],
            indexRule.AllowedTypes);
        Assert.Contains("equality", indexRule.AllowedValues);
    }

    [Fact]
    public void EmbeddedCapabilities_AreBoundToThe440ReleaseAssembliesAndResource()
    {
        const string expectedVersion = "4.4.0";
        Assembly migrationAssembly = typeof(CSharpDbCapabilityCatalogLoader).Assembly;
        Assembly primitivesAssembly = typeof(DbType).Assembly;

        Assert.Equal(expectedVersion, CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);
        Assert.Equal(expectedVersion, InformationalVersion(migrationAssembly));
        Assert.Equal(expectedVersion, InformationalVersion(primitivesAssembly));
        Assert.Contains(
            $"CSharpDB.Migration.Capabilities.csharpdb-{expectedVersion}.json",
            migrationAssembly.GetManifestResourceNames());
    }

    [Fact]
    public void EmbeddedCapabilities_RetainTheImmutablePreviousReleaseContract()
    {
        CSharpDbCapabilityCatalog previous =
            CSharpDbCapabilityCatalogLoader.LoadEmbedded("4.3.0");
        CSharpDbCapabilityCatalog current =
            CSharpDbCapabilityCatalogLoader.LoadEmbedded();

        Assert.Equal(["4.3.0", "4.4.0"], CSharpDbCapabilityCatalogLoader.SupportedTargetVersions);
        Assert.Equal("4.3.0", previous.TargetCSharpDbVersion);
        Assert.Equal("4.4.0", current.TargetCSharpDbVersion);
        Assert.NotEqual(previous.Digest, current.Digest);

        CSharpDbCapabilityRule previousForeignKey = previous.Rules.Single(rule =>
            rule.Feature == CSharpDbCapabilityFeature.ForeignKey);
        Assert.DoesNotContain("on-update-cascade", previousForeignKey.AllowedValues);
        Assert.Contains(
            "on-update-cascade",
            current.Rules.Single(rule =>
                rule.Feature == CSharpDbCapabilityFeature.ForeignKey).AllowedValues);

        CSharpDbCapabilityRule previousIndex = previous.Rules.Single(rule =>
            rule.Feature == CSharpDbCapabilityFeature.Index);
        Assert.DoesNotContain(DbType.Real, previousIndex.AllowedTypes);
        Assert.Contains(
            DbType.Real,
            current.Rules.Single(rule =>
                rule.Feature == CSharpDbCapabilityFeature.Index).AllowedTypes);
    }

    [Fact]
    public async Task SyntheticInspection_RetainsAwkwardAndUnsupportedSourceFacts()
    {
        MigrationCatalog catalog = await InspectAsync();

        Assert.Contains(catalog.Objects, item => item.SourceName == "Order \"Lines\" 🚀");
        Assert.Contains(catalog.Objects, item => item.SourceName == "sys_tables");
        Assert.Contains(catalog.Objects, item => item.SourceName.Length > SqlIdentifierRules.MaxLength);
        MigrationCatalogObject geography = Assert.Single(
            catalog.Objects,
            item => item.NativeType == "GEOGRAPHY");
        Assert.Contains(geography.Facets, facet => facet.Name == "srid" && facet.Value == "4326");
        Assert.Contains(
            catalog.Diagnostics,
            item => item.DiagnosticId == "syn:diag:trigger-when" &&
                    item.Status == MigrationCompatibilityStatus.Unsupported);

        string serialized = MigrationArtifactSerializer.SerializeCatalog(catalog);
        MigrationCatalog restored = MigrationArtifactSerializer.DeserializeCatalog(serialized);
        Assert.Equal(serialized, MigrationArtifactSerializer.SerializeCatalog(restored));
    }

    [Fact]
    public async Task PreservePlan_IsDeterministicAndClassifiesEveryScalar()
    {
        MigrationCatalog catalog = await InspectAsync();
        var planner = new MigrationPlanner();

        MigrationPlan plan = planner.CreatePlan(catalog);
        string first = MigrationArtifactSerializer.SerializePlan(plan, catalog);
        string second = MigrationArtifactSerializer.SerializePlan(planner.CreatePlan(catalog), catalog);

        Assert.Equal(first, second);
        Assert.Equal(catalog.Objects.Count, plan.Objects.Count);
        Assert.Equal(
            catalog.Objects.Count(item => item.NativeType is not null),
            plan.Objects.Sum(item => item.TypeMappings.Count));
        Assert.Contains(AllMappings(plan), item => item.Classification == MigrationMappingClassification.Exact);
        Assert.Contains(AllMappings(plan), item => item.Classification == MigrationMappingClassification.LosslessReencoded);
        Assert.Contains(AllMappings(plan), item => item.Classification == MigrationMappingClassification.Unsupported);
        Assert.DoesNotContain(AllMappings(plan), item => item.Classification == MigrationMappingClassification.Lossy);

        MigrationTypeMapping decimalMapping = Mapping(plan, "syn:column:orders:amount");
        Assert.Equal(DbType.Text, decimalMapping.TargetType);
        Assert.Equal("decimal-text", decimalMapping.Conversion!.ConversionId);
        Assert.Equal(MigrationMappingClassification.LosslessReencoded, decimalMapping.Classification);

        MigrationTypeMapping scaledDecimal = Mapping(plan, "syn:column:orders:tax");
        Assert.Equal(DbType.Integer, scaledDecimal.TargetType);
        Assert.Equal("decimal-scaled-int64", scaledDecimal.Conversion!.ConversionId);

        MigrationPlanObject spatial = Object(plan, "syn:table:spatial");
        MigrationPlanObject geography = Object(plan, "syn:column:spatial:shape");
        Assert.False(spatial.Included);
        Assert.False(geography.Included);
        Assert.Equal(MigrationMappingClassification.Unsupported, geography.TypeMappings[0].Classification);
        MigrationPlanReadiness readiness = MigrationPlanReadinessValidator.Evaluate(plan, catalog);
        Assert.Equal(MigrationPlanReadinessStatus.RequiresApproval, readiness.Status);
        Assert.NotEmpty(readiness.PendingExclusionObjectIds);
        Assert.Empty(readiness.BlockingDiagnosticIds);
    }

    [Fact]
    public void DecimalLookalikeFacetDoesNotActivateTypedJsonConversion()
    {
        var source = new MigrationCatalogObject
        {
            ObjectId = "test:column:amount",
            Kind = MigrationObjectKind.Column,
            SourceName = "amount",
            NativeType = "UNRELATED_DECIMAL",
            Facets =
            [
                new MigrationCatalogFacet
                {
                    Name = "logicalType",
                    Value = "decimal",
                },
                new MigrationCatalogFacet
                {
                    Name = "precision",
                    Value = "38",
                },
                new MigrationCatalogFacet
                {
                    Name = "scale",
                    Value = "18",
                },
                new MigrationCatalogFacet
                {
                    Name = "jsonTypedCodec",
                    Value = "decimalString",
                },
            ],
        };

        MigrationTypeMapping mapping =
            new StandardDataTypeMappingProvider().Map(
                new MigrationTypeMappingRequest
                {
                    SourceObject = source,
                    Profile =
                        MigrationMappingProfile.Preserve,
                    Coverage = new MigrationProfileCoverage
                    {
                        Kind = MigrationCoverageKind.Full,
                        ValuesExamined = 1,
                        TotalValues = 1,
                        RequiresFullStreamValidation =
                            false,
                    },
                }).Mapping;

        Assert.Equal(DbType.Text, mapping.TargetType);
        Assert.Equal(
            "decimal-text",
            mapping.Conversion?.ConversionId);
    }

    [Fact]
    public async Task DefaultLoadPolicy_PreservesCanonicalPlanJsonAndDigest()
    {
        MigrationCatalog catalog = await InspectAsync();
        var planner = new MigrationPlanner();

        MigrationPlan implicitDefault = planner.CreatePlan(catalog);
        MigrationPlan emptyOptions = planner.CreatePlan(
            catalog,
            new MigrationPlanningOptions());
        MigrationPlan explicitDefault = planner.CreatePlan(
            catalog,
            new MigrationPlanningOptions { Load = new MigrationLoadPolicy() });

        string expectedJson = MigrationArtifactSerializer.SerializePlan(
            implicitDefault,
            catalog,
            writeIndented: false);
        string expectedDigest = MigrationArtifactSerializer.ComputePlanDigest(implicitDefault);

        Assert.Equal(new MigrationLoadPolicy(), implicitDefault.Load);
        Assert.Equal(
            expectedJson,
            MigrationArtifactSerializer.SerializePlan(
                emptyOptions,
                catalog,
                writeIndented: false));
        Assert.Equal(
            expectedJson,
            MigrationArtifactSerializer.SerializePlan(
                explicitDefault,
                catalog,
                writeIndented: false));
        Assert.Equal(expectedDigest, MigrationArtifactSerializer.ComputePlanDigest(emptyOptions));
        Assert.Equal(expectedDigest, MigrationArtifactSerializer.ComputePlanDigest(explicitDefault));
    }

    [Fact]
    public async Task ExplicitLoadPolicy_IsValidatedAndBoundIntoThePlanDigest()
    {
        MigrationCatalog catalog = await InspectAsync();
        var loadPolicy = new MigrationLoadPolicy
        {
            BatchSize = 32,
            MaxBatchBytes = 1024 * 1024,
            MaxValueBytes = 64 * 1024,
            RejectMode = MigrationRejectMode.DeterministicRejects,
            RejectPolicy = new MigrationDeterministicRejectPolicy
            {
                ContractVersion = MigrationRejectContract.DeterministicRejectsV1,
                AllowedRuleIds = ["MIG-TEST-001"],
                MaxRejectedRowsPerBatch = 4,
                MaxRejectedRowsPerRun = 10,
                MaxRawValueBytes = 1_024,
                MaxRawValueBytesPerBatch = 4_096,
                MaxRawValueBytesPerRun = 8_192,
                MaxArtifactBytes = 131_072,
            },
        };
        var planner = new MigrationPlanner();
        MigrationPlan strict = planner.CreatePlan(catalog);

        MigrationPlan deterministic = planner.CreatePlan(
            catalog,
            new MigrationPlanningOptions { Load = loadPolicy });
        string json = MigrationArtifactSerializer.SerializePlan(
            deterministic,
            catalog,
            writeIndented: false);
        MigrationPlan restored = MigrationArtifactSerializer.DeserializePlan(json, catalog);

        Assert.Same(loadPolicy, deterministic.Load);
        Assert.Equal(MigrationRejectMode.DeterministicRejects, restored.Load.RejectMode);
        Assert.Equal(32, restored.Load.BatchSize);
        Assert.Equal(loadPolicy.MaxBatchBytes, restored.Load.MaxBatchBytes);
        Assert.Equal(loadPolicy.MaxValueBytes, restored.Load.MaxValueBytes);
        Assert.Equal(
            loadPolicy.RejectPolicy!.ContractVersion,
            restored.Load.RejectPolicy!.ContractVersion);
        Assert.Equal(
            loadPolicy.RejectPolicy.AllowedRuleIds,
            restored.Load.RejectPolicy.AllowedRuleIds);
        Assert.Equal(
            loadPolicy.RejectPolicy.MaxRejectedRowsPerBatch,
            restored.Load.RejectPolicy.MaxRejectedRowsPerBatch);
        Assert.Equal(
            loadPolicy.RejectPolicy.MaxRejectedRowsPerRun,
            restored.Load.RejectPolicy.MaxRejectedRowsPerRun);
        Assert.Equal(
            loadPolicy.RejectPolicy.MaxRawValueBytes,
            restored.Load.RejectPolicy.MaxRawValueBytes);
        Assert.Equal(
            loadPolicy.RejectPolicy.MaxRawValueBytesPerBatch,
            restored.Load.RejectPolicy.MaxRawValueBytesPerBatch);
        Assert.Equal(
            loadPolicy.RejectPolicy.MaxRawValueBytesPerRun,
            restored.Load.RejectPolicy.MaxRawValueBytesPerRun);
        Assert.Equal(
            loadPolicy.RejectPolicy.MaxArtifactBytes,
            restored.Load.RejectPolicy.MaxArtifactBytes);
        Assert.NotEqual(
            MigrationArtifactSerializer.ComputePlanDigest(strict),
            MigrationArtifactSerializer.ComputePlanDigest(restored));
    }

    [Fact]
    public async Task NameMapping_HandlesCaseCollisionsReservedNamesAndLengthWithoutLosingUnicode()
    {
        MigrationCatalog catalog = await InspectAsync();
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);

        string upper = Object(plan, "syn:table:customers-upper").TargetName!;
        string lower = Object(plan, "syn:table:customers-lower").TargetName!;
        Assert.False(string.Equals(upper, lower, StringComparison.OrdinalIgnoreCase));
        Assert.Contains("__", upper, StringComparison.Ordinal);
        Assert.Contains("__", lower, StringComparison.Ordinal);

        string codeUpper = Object(plan, "syn:column:customers-lower:code-upper").TargetName!;
        string codeLower = Object(plan, "syn:column:customers-lower:code-lower").TargetName!;
        Assert.False(string.Equals(codeUpper, codeLower, StringComparison.OrdinalIgnoreCase));

        string reserved = Object(plan, "syn:table:reserved").TargetName!;
        Assert.StartsWith("migrated_sys_tables__", reserved, StringComparison.Ordinal);

        string orders = Object(plan, "syn:table:orders").TargetName!;
        Assert.Equal("sales__Order \"Lines\" 🚀", orders);

        IReadOnlyDictionary<string, string> names = DeterministicMigrationNameMapper.Map(catalog);
        string longName = names["syn:table:spatial"];
        Assert.True(longName.Length <= SqlIdentifierRules.MaxLength);
        Assert.Matches("__[0-9a-f]{16}$", longName);
        Assert.Equal(
            longName,
            DeterministicMigrationNameMapper.Map(catalog)["syn:table:spatial"]);
    }

    [Fact]
    public async Task QueryablePlan_SerializesPendingLossyChoicesForPreview()
    {
        MigrationCatalog catalog = await InspectAsync();
        MigrationPlan plan = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions { MappingProfile = MigrationMappingProfile.Queryable });

        MigrationTypeMapping decimalMapping = Mapping(plan, "syn:column:orders:amount");
        MigrationTypeMapping unsignedMapping = Mapping(plan, "syn:column:orders:source-counter");
        Assert.Equal(MigrationMappingClassification.Lossy, decimalMapping.Classification);
        Assert.Equal(MigrationMappingClassification.Lossy, unsignedMapping.Classification);
        Assert.Equal(MigrationCoverageKind.Sample, unsignedMapping.Coverage.Kind);
        Assert.True(unsignedMapping.Coverage.RequiresFullStreamValidation);

        string json = MigrationArtifactSerializer.SerializePlan(plan, catalog);
        MigrationPlan restored = MigrationArtifactSerializer.DeserializePlan(json, catalog);
        MigrationPlanReadiness readiness = MigrationPlanReadinessValidator.Evaluate(restored, catalog);
        Assert.Equal(2, readiness.PendingDiagnosticIds.Count);
        Assert.NotEmpty(readiness.PendingExclusionObjectIds);
        Assert.Empty(readiness.BlockingDiagnosticIds);
    }

    [Fact]
    public async Task SampleCoverage_AllowsAnUnknownTotalWithoutInventingOne()
    {
        MigrationCatalog catalog = await InspectAsync();
        const string objectId = "syn:column:orders:source-counter";
        MigrationCatalogObject column = catalog.Objects.Single(item => item.ObjectId == objectId);
        catalog = catalog with
        {
            Objects = catalog.Objects
                .Select(item => item.ObjectId == objectId
                    ? column with
                    {
                        Facets = column.Facets
                            .Where(facet => facet.Name != "profileTotalValues")
                            .ToArray(),
                    }
                    : item)
                .ToArray(),
        };

        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        MigrationProfileCoverage coverage = Mapping(plan, objectId).Coverage;

        Assert.Equal(MigrationCoverageKind.Sample, coverage.Kind);
        Assert.Equal(5, coverage.ValuesExamined);
        Assert.Null(coverage.TotalValues);
        Assert.True(coverage.RequiresFullStreamValidation);
    }

    [Fact]
    public async Task FullCoverage_StillRequiresAnExactTotal()
    {
        MigrationCatalog catalog = await InspectAsync();
        const string objectId = "syn:column:orders:amount";
        MigrationCatalogObject column = catalog.Objects.Single(item => item.ObjectId == objectId);
        catalog = catalog with
        {
            Objects = catalog.Objects
                .Select(item => item.ObjectId == objectId
                    ? column with
                    {
                        Facets = column.Facets
                            .Where(facet => facet.Name != "profileTotalValues")
                            .ToArray(),
                    }
                    : item)
                .ToArray(),
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => new MigrationPlanner().CreatePlan(catalog));

        Assert.Contains("profileTotalValues", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SyntheticPlanningArtifacts_MatchGoldenDigestVectors()
    {
        MigrationCatalog catalog = await InspectAsync();
        var planner = new MigrationPlanner();
        MigrationPlan preserve = planner.CreatePlan(catalog);
        MigrationPlan queryable = planner.CreatePlan(
            catalog,
            new MigrationPlanningOptions { MappingProfile = MigrationMappingProfile.Queryable });

        using JsonDocument golden = JsonDocument.Parse(
            File.ReadAllText(Path.Combine(
                AppContext.BaseDirectory,
                "Fixtures",
                "synthetic-planning-v1.golden.json")));
        JsonElement root = golden.RootElement;

        string catalogDigest =
            ReadArtifactDigest(MigrationArtifactSerializer.SerializeCatalog(catalog));
        string preservePlanDigest =
            ReadArtifactDigest(MigrationArtifactSerializer.SerializePlan(preserve, catalog));
        string queryablePlanDigest =
            ReadArtifactDigest(MigrationArtifactSerializer.SerializePlan(queryable, catalog));

        Assert.True(
            string.Equals(
                root.GetProperty("catalogDigest").GetString(),
                catalogDigest,
                StringComparison.Ordinal) &&
            string.Equals(
                root.GetProperty("preservePlanDigest").GetString(),
                preservePlanDigest,
                StringComparison.Ordinal) &&
            string.Equals(
                root.GetProperty("queryablePlanDigest").GetString(),
                queryablePlanDigest,
                StringComparison.Ordinal),
            "Synthetic planning golden digests changed. Actual values: " +
            $"catalog={catalogDigest}, preserve={preservePlanDigest}, " +
            $"queryable={queryablePlanDigest}.");
    }

    [Fact]
    public async Task CustomPlan_UsesExplicitTargetAndPreserveFallbacks()
    {
        MigrationCatalog catalog = await InspectAsync();
        MigrationPlan plan = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions
            {
                MappingProfile = MigrationMappingProfile.Custom,
                CustomTargetTypes = new Dictionary<string, DbType>(StringComparer.Ordinal)
                {
                    ["syn:column:orders:amount"] = DbType.Real,
                },
            });

        Assert.Equal(
            MigrationMappingClassification.Lossy,
            Mapping(plan, "syn:column:orders:amount").Classification);
        Assert.Equal(
            MigrationMappingClassification.LosslessReencoded,
            Mapping(plan, "syn:column:orders:ordered-at").Classification);
        Assert.All(AllMappings(plan), mapping => Assert.Equal(MigrationMappingProfile.Custom, mapping.Profile));
    }

    [Fact]
    public async Task Plan_RejectsCapabilityDigestMismatchAndNullColumnType()
    {
        MigrationCatalog catalog = await InspectAsync();
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        InvalidDataException digestError = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(
                plan with { CapabilityDigest = new string('a', 64) },
                catalog));
        Assert.Contains("capability digest", digestError.Message, StringComparison.OrdinalIgnoreCase);

        MigrationPlanObject scalar = plan.Objects.First(item => item.TypeMappings.Count == 1 && item.Included);
        MigrationTypeMapping invalidMapping = scalar.TypeMappings[0] with { TargetType = DbType.Null };
        MigrationPlan invalidPlan = plan with
        {
            Objects = plan.Objects
                .Select(item => item.SourceObjectId == scalar.SourceObjectId
                    ? item with { TypeMappings = [invalidMapping] }
                    : item)
                .ToArray(),
        };
        InvalidDataException typeError = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(invalidPlan, catalog));
        Assert.Contains("persistent target column", typeError.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Plan_RejectsTargetNameThatDoesNotMatchBoundAlgorithm()
    {
        MigrationCatalog catalog = await InspectAsync();
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        MigrationPlanObject orders = Object(plan, "syn:table:orders");
        MigrationPlan tampered = Replace(
            plan,
            orders with { TargetName = "manually-renamed-orders" });

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(tampered, catalog));

        Assert.Contains("bound naming algorithm", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Plan_RejectsRemovedCatalogDependency()
    {
        MigrationCatalog catalog = await InspectAsync();
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        MigrationPlanObject index = Object(plan, "syn:index:orders:amount");
        Assert.True(index.Included);
        MigrationPlan tampered = Replace(plan, index with { DependsOn = [] });

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(tampered, catalog));

        Assert.Contains("dependencies do not match", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Plan_RejectsReincludedUnsupportedTargetObject()
    {
        MigrationCatalog catalog = await InspectAsync();
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        MigrationPlanObject sequence = Object(plan, "syn:sequence:orders");
        IReadOnlyDictionary<string, string> names = DeterministicMigrationNameMapper.Map(catalog);
        MigrationPlan tampered = Replace(
            plan,
            sequence with
            {
                Included = true,
                ExclusionReason = null,
                TargetName = names[sequence.SourceObjectId],
            });

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(tampered, catalog));

        Assert.Contains("not supported", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static async Task<MigrationCatalog> InspectAsync() =>
        await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            });

    private static MigrationPlanObject Object(MigrationPlan plan, string objectId) =>
        plan.Objects.Single(item => item.SourceObjectId == objectId);

    private static MigrationTypeMapping Mapping(MigrationPlan plan, string objectId) =>
        Assert.Single(Object(plan, objectId).TypeMappings);

    private static IReadOnlyList<MigrationTypeMapping> AllMappings(MigrationPlan plan) =>
        plan.Objects.SelectMany(item => item.TypeMappings).ToArray();

    private static MigrationPlan Replace(MigrationPlan plan, MigrationPlanObject replacement) =>
        plan with
        {
            Objects = plan.Objects
                .Select(item => item.SourceObjectId == replacement.SourceObjectId ? replacement : item)
                .ToArray(),
        };

    private static string InformationalVersion(Assembly assembly) =>
        assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()!
            .InformationalVersion
            .Split('+', 2)[0];

    private static string ReadArtifactDigest(string json)
    {
        using JsonDocument document = JsonDocument.Parse(json);
        return document.RootElement.GetProperty("digest").GetString()!;
    }
}
