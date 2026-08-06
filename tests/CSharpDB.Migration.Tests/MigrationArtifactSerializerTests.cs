using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationArtifactSerializerTests
{
    private static readonly JsonSerializerOptions DigestOptions = CreateDigestOptions();

    [Fact]
    public void Catalog_RoundTripsWithDeterministicDigestAndGoldenJson()
    {
        MigrationCatalog catalog = CreateCatalog();

        string first = MigrationArtifactSerializer.SerializeCatalog(catalog);
        string second = MigrationArtifactSerializer.SerializeCatalog(catalog);
        Assert.Equal(first, second);
        Assert.Equal(ReadGolden("catalog-v1.golden.json"), NormalizeLineEndings(first));

        MigrationCatalog restored = MigrationArtifactSerializer.DeserializeCatalog(first);
        Assert.Equal(first, MigrationArtifactSerializer.SerializeCatalog(restored));
    }

    [Fact]
    public void Catalog_DigestDoesNotDependOnPrettyPrinting()
    {
        MigrationCatalog catalog = CreateCatalog();
        string indented = MigrationArtifactSerializer.SerializeCatalog(catalog, writeIndented: true);
        string compact = MigrationArtifactSerializer.SerializeCatalog(catalog, writeIndented: false);

        using JsonDocument indentedDocument = JsonDocument.Parse(indented);
        using JsonDocument compactDocument = JsonDocument.Parse(compact);

        string indentedDigest = indentedDocument.RootElement.GetProperty("digest").GetString()!;
        string compactDigest = compactDocument.RootElement.GetProperty("digest").GetString()!;
        Assert.Equal(indentedDigest, compactDigest);
    }

    [Fact]
    public void Catalog_RejectsPayloadTampering()
    {
        string json = MigrationArtifactSerializer.SerializeCatalog(CreateCatalog());
        string tampered = json.Replace("customers", "customers_changed", StringComparison.Ordinal);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.DeserializeCatalog(tampered));

        Assert.Contains("digest does not match", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Catalog_RejectsUnknownMembers()
    {
        JsonObject envelope = JsonNode.Parse(
            MigrationArtifactSerializer.SerializeCatalog(CreateCatalog()))!.AsObject();
        envelope["payload"]!.AsObject()["futureField"] = true;
        string tampered = Redigest(envelope);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.DeserializeCatalog(tampered));

        Assert.Contains("invalid", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Catalog_RejectsCredentialAssignments()
    {
        MigrationCatalog source = CreateCatalog();
        MigrationCatalog catalog = source with
        {
            Source = source.Source with
            {
                Identity = "Server=localhost;Password=do-not-store",
            },
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializeCatalog(catalog));

        Assert.Contains("credential material", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("https://admin:do-not-store@example.test/database")]
    [InlineData("Authorization: Bearer do-not-store")]
    [InlineData("-----BEGIN PRIVATE KEY-----")]
    public void Catalog_RejectsOtherCommonCredentialShapes(string identity)
    {
        MigrationCatalog source = CreateCatalog();
        MigrationCatalog catalog = source with
        {
            Source = source.Source with { Identity = identity },
        };

        Assert.Throws<InvalidDataException>(() => MigrationArtifactSerializer.SerializeCatalog(catalog));
    }

    [Fact]
    public void Catalog_RejectsDuplicatePropertyEvenWhenLastValueAndDigestAreSafe()
    {
        string json = MigrationArtifactSerializer.SerializeCatalog(CreateCatalog());
        string duplicated = json.Replace(
            "\"identity\": \"fixture:awkward-v1\"",
            "\"identity\": \"Password=do-not-store\",\n      \"identity\": \"fixture:awkward-v1\"",
            StringComparison.Ordinal);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.DeserializeCatalog(duplicated));

        Assert.Contains("duplicate property", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Catalog_NormalizesSetLikeCollectionsBeforeHashing()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationCatalogObject column = catalog.Objects[1];
        MigrationCatalog reordered = catalog with
        {
            Objects =
            [
                column with { Facets = column.Facets.Reverse().ToArray() },
                catalog.Objects[0],
            ],
        };

        Assert.Equal(
            MigrationArtifactSerializer.SerializeCatalog(catalog),
            MigrationArtifactSerializer.SerializeCatalog(reordered));
    }

    [Fact]
    public void Catalog_RejectsSecretBearingFacetKeys()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationCatalogObject table = catalog.Objects[0];
        MigrationCatalog unsafeCatalog = catalog with
        {
            Objects =
            [
                table with
                {
                    Facets = [new MigrationCatalogFacet { Name = "password", Value = "do-not-store" }],
                },
                catalog.Objects[1],
            ],
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializeCatalog(unsafeCatalog));

        Assert.Contains("secret-bearing key", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Mapping_RecordsSampleCoverageAndFullStreamRequirement()
    {
        var mapping = new MigrationTypeMapping
        {
            SourceObjectId = "table:customers/column:external_id",
            SourceNativeType = "NUMERIC",
            TargetType = CSharpDB.Primitives.DbType.Integer,
            Classification = MigrationMappingClassification.Exact,
            Profile = MigrationMappingProfile.Preserve,
            Coverage = new MigrationProfileCoverage
            {
                Kind = MigrationCoverageKind.Sample,
                ValuesExamined = 1_000,
                TotalValues = 50_000,
                RequiresFullStreamValidation = true,
            },
        };

        Assert.Equal(MigrationCoverageKind.Sample, mapping.Coverage.Kind);
        Assert.True(mapping.Coverage.RequiresFullStreamValidation);
        Assert.Equal(1_000, mapping.Coverage.ValuesExamined);
        Assert.Equal(50_000, mapping.Coverage.TotalValues);
    }

    [Fact]
    public void Plan_RejectsSampleDerivedMappingWithoutFullStreamValidation()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = CreatePlan(catalog, new MigrationProfileCoverage
        {
            Kind = MigrationCoverageKind.Sample,
            ValuesExamined = 100,
            TotalValues = 1_000,
            RequiresFullStreamValidation = false,
        });

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(plan, catalog));

        Assert.Contains("full-stream validation", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Plan_RoundTripsWhenSampleMappingRequiresFullStreamValidation()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = CreatePlan(catalog, new MigrationProfileCoverage
        {
            Kind = MigrationCoverageKind.Sample,
            ValuesExamined = 100,
            TotalValues = 1_000,
            RequiresFullStreamValidation = true,
        });

        string json = MigrationArtifactSerializer.SerializePlan(plan, catalog);
        MigrationPlan restored = MigrationArtifactSerializer.DeserializePlan(json, catalog);

        Assert.Equal(json, MigrationArtifactSerializer.SerializePlan(restored, catalog));
    }

    [Fact]
    public void Plan_RejectsUnknownMembers()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = CreatePlan(catalog, new MigrationProfileCoverage
        {
            Kind = MigrationCoverageKind.None,
            ValuesExamined = 0,
            RequiresFullStreamValidation = true,
        });
        JsonObject envelope = JsonNode.Parse(
            MigrationArtifactSerializer.SerializePlan(plan, catalog))!.AsObject();
        envelope["payload"]!.AsObject()["futureField"] = true;

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.DeserializePlan(Redigest(envelope), catalog));

        Assert.Contains("invalid", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Plan_RejectsDuplicatePropertyEvenWhenLastValueIsValid()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = CreatePlan(catalog, new MigrationProfileCoverage
        {
            Kind = MigrationCoverageKind.None,
            ValuesExamined = 0,
            RequiresFullStreamValidation = true,
        });
        string json = MigrationArtifactSerializer.SerializePlan(plan, catalog);
        string duplicated = json.Replace(
            "\"mappingProfile\": \"preserve\"",
            "\"mappingProfile\": \"queryable\",\n    \"mappingProfile\": \"preserve\"",
            StringComparison.Ordinal);

        Assert.NotEqual(json, duplicated);
        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.DeserializePlan(duplicated, catalog));

        Assert.Contains("duplicate property", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Plan_RejectsCredentialMaterialInDiagnosticText()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = CreatePlan(catalog, new MigrationProfileCoverage
        {
            Kind = MigrationCoverageKind.None,
            ValuesExamined = 0,
            RequiresFullStreamValidation = true,
        }) with
        {
            Diagnostics =
            [
                .. catalog.Diagnostics,
                new MigrationDiagnostic
                {
                    DiagnosticId = "plan:diag:unsafe",
                    RuleId = "MIG-SEC-0001",
                    Severity = MigrationDiagnosticSeverity.Warning,
                    Status = MigrationCompatibilityStatus.Conditional,
                    Evidence = MigrationEvidenceLevel.Parsed,
                    Summary = "Password=do-not-store",
                    Explanation = "Unsafe diagnostic fixture.",
                    ObjectId = "table:customers",
                    CanOverride = false,
                },
            ],
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(plan, catalog));

        Assert.Contains("credential material", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Plan_RejectsSecretBearingConversionParameters()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = CreatePlan(catalog, new MigrationProfileCoverage
        {
            Kind = MigrationCoverageKind.None,
            ValuesExamined = 0,
            RequiresFullStreamValidation = true,
        });
        JsonObject envelope = JsonNode.Parse(
            MigrationArtifactSerializer.SerializePlan(plan, catalog))!.AsObject();
        JsonObject column = envelope["payload"]!["objects"]!.AsArray()
            .Select(item => item!.AsObject())
            .Single(item => item["typeMappings"]!.AsArray().Count == 1);
        JsonArray parameters = column["typeMappings"]![0]!["conversion"]!["parameters"]!.AsArray();
        parameters.Add(new JsonObject
        {
            ["name"] = "password",
            ["value"] = "do-not-store",
        });

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.DeserializePlan(Redigest(envelope), catalog));

        Assert.Contains("secret-bearing key", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Plan_RejectsFullCoverageWithoutKnownMatchingTotal()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = CreatePlan(catalog, new MigrationProfileCoverage
        {
            Kind = MigrationCoverageKind.Full,
            ValuesExamined = 1,
            TotalValues = null,
            RequiresFullStreamValidation = false,
        });

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(plan, catalog));

        Assert.Contains("must report its total", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Plan_SerializesLossyMappingWithoutAcceptedDiagnosticInstanceForPreview()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = CreateLossyPlan(catalog, accepted: false);

        string json = MigrationArtifactSerializer.SerializePlan(plan, catalog);
        MigrationPlan restored = MigrationArtifactSerializer.DeserializePlan(json, catalog);

        Assert.Empty(restored.AcceptedDiagnosticIds);
        Assert.Equal(
            MigrationMappingClassification.Lossy,
            restored.Objects.SelectMany(item => item.TypeMappings).Single().Classification);
    }

    [Fact]
    public void ApplyReadiness_RejectsLossyMappingWithoutAcceptedDiagnosticInstance()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = CreateLossyPlan(catalog, accepted: false);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationPlanReadinessValidator.ValidateForApply(plan, catalog));

        Assert.Contains("before apply", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Plan_AllowsLossyMappingWithExplicitOverrideableDiagnosticInstance()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = CreateLossyPlan(catalog, accepted: true);

        string json = MigrationArtifactSerializer.SerializePlan(plan, catalog);
        MigrationPlan restored = MigrationArtifactSerializer.DeserializePlan(json, catalog);
        MigrationPlanReadinessValidator.ValidateForApply(restored, catalog);

        Assert.Single(restored.AcceptedDiagnosticIds);
        Assert.Equal(plan.AcceptedDiagnosticIds[0], restored.AcceptedDiagnosticIds[0]);
    }

    [Fact]
    public void Plan_RejectsWrongCatalogDigest()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = CreatePlan(catalog, new MigrationProfileCoverage
        {
            Kind = MigrationCoverageKind.None,
            ValuesExamined = 0,
            RequiresFullStreamValidation = true,
        }) with
        {
            CatalogDigest = new string('b', 64),
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(plan, catalog));

        Assert.Contains("does not match", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Plan_RejectsMappingThatDoesNotMatchItsBoundPolicy()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = CreatePlan(catalog, new MigrationProfileCoverage
        {
            Kind = MigrationCoverageKind.None,
            ValuesExamined = 0,
            RequiresFullStreamValidation = true,
        });
        MigrationPlanObject column = plan.Objects.Single(item => item.TypeMappings.Count == 1);
        MigrationTypeMapping promoted = column.TypeMappings[0] with
        {
            TargetType = CSharpDB.Primitives.DbType.Integer,
            Classification = MigrationMappingClassification.Exact,
            Conversion = null,
        };
        plan = plan with
        {
            Objects = plan.Objects
                .Select(item => item.SourceObjectId == column.SourceObjectId
                    ? column with { TypeMappings = [promoted] }
                    : item)
                .ToArray(),
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(plan, catalog));

        Assert.Contains("does not match mapping policy", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Plan_RejectsUnknownMappingPolicyVersion()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = CreatePlan(catalog, new MigrationProfileCoverage
        {
            Kind = MigrationCoverageKind.None,
            ValuesExamined = 0,
            RequiresFullStreamValidation = true,
        }) with
        {
            MappingPolicyVersion = 999,
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(plan, catalog));

        Assert.Contains("mapping policy", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Plan_CustomMappingPolicyMustBeSuppliedForValidationAndReplay()
    {
        MigrationCatalog catalog = CreateCatalog();
        var policy = new DelegatingMappingPolicy();
        MigrationPlan plan = new MigrationPlanner(typeMapper: policy).CreatePlan(catalog);

        InvalidDataException missing = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(plan, catalog));
        Assert.Contains("not registered", missing.Message, StringComparison.OrdinalIgnoreCase);

        string json = MigrationArtifactSerializer.SerializePlan(plan, catalog, policy);
        MigrationPlan restored = MigrationArtifactSerializer.DeserializePlan(json, catalog, policy);

        Assert.Equal(policy.PolicyId, restored.MappingPolicyId);
        Assert.Equal(policy.PolicyVersion, restored.MappingPolicyVersion);
        Assert.Equal(json, MigrationArtifactSerializer.SerializePlan(restored, catalog, policy));
    }

    [Fact]
    public void Plan_CannotDropCatalogDiagnostics()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = CreatePlan(catalog, new MigrationProfileCoverage
        {
            Kind = MigrationCoverageKind.None,
            ValuesExamined = 0,
            RequiresFullStreamValidation = true,
        }) with
        {
            Diagnostics = [],
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(plan, catalog));

        Assert.Contains("retain catalog diagnostic", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Plan_RejectsIncludedObjectWithUnsupportedMapping()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = CreatePlan(catalog, new MigrationProfileCoverage
        {
            Kind = MigrationCoverageKind.None,
            ValuesExamined = 0,
            RequiresFullStreamValidation = true,
        });
        MigrationPlanObject planObject = plan.Objects.Single(item => item.TypeMappings.Count == 1);
        MigrationTypeMapping mapping = planObject.TypeMappings[0] with
        {
            TargetType = null,
            Classification = MigrationMappingClassification.Unsupported,
            DiagnosticId = catalog.Diagnostics[0].DiagnosticId,
        };
        plan = plan with
        {
            Objects = plan.Objects
                .Select(item => item.SourceObjectId == planObject.SourceObjectId
                    ? planObject with { TypeMappings = [mapping] }
                    : item)
                .ToArray(),
            Diagnostics = [catalog.Diagnostics[0]],
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(plan, catalog));

        Assert.Contains("contains unsupported mapping", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Plan_RejectsMappingProfileThatDoesNotMatchPlanProfile()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = CreatePlan(catalog, new MigrationProfileCoverage
        {
            Kind = MigrationCoverageKind.None,
            ValuesExamined = 0,
            RequiresFullStreamValidation = true,
        }) with
        {
            MappingProfile = MigrationMappingProfile.Queryable,
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(plan, catalog));

        Assert.Contains("plan uses", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Plan_CannotPromoteCatalogDiagnosticToOverrideable()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = CreatePlan(catalog, new MigrationProfileCoverage
        {
            Kind = MigrationCoverageKind.None,
            ValuesExamined = 0,
            RequiresFullStreamValidation = true,
        }) with
        {
            Diagnostics = [catalog.Diagnostics[0] with { CanOverride = true }],
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializePlan(plan, catalog));

        Assert.Contains("does not match", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Catalog_RejectsExplicitNullCollectionsAsContractErrors()
    {
        MigrationCatalog catalog = CreateCatalog() with { Objects = null! };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationArtifactSerializer.SerializeCatalog(catalog));

        Assert.Contains("cannot be null", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static MigrationCatalog CreateCatalog() => new()
    {
        TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
        Source = new MigrationSourceIdentity
        {
            Kind = MigrationSourceKind.Synthetic,
            Identity = "fixture:awkward-v1",
            Fingerprint = "sha256:6f7b4f1b2f9e8f6eb164fae886f20ed840cbe2d838056f108f9c1fdb5e7813ad",
            ProviderVersion = "1.0",
            SourceVersion = "fixture-v1",
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Immutable,
                Description = "Versioned immutable test fixture.",
            },
        },
        Objects =
        [
            new MigrationCatalogObject
            {
                ObjectId = "table:customers",
                Kind = MigrationObjectKind.Table,
                SourceNamespace = "main",
                SourceName = "customers",
                Facets =
                [
                    new MigrationCatalogFacet { Name = "strict", Value = "false" },
                ],
            },
            new MigrationCatalogObject
            {
                ObjectId = "table:customers/column:id",
                Kind = MigrationObjectKind.Column,
                SourceNamespace = "main",
                SourceName = "id",
                NativeType = "UNSIGNED BIGINT",
                ParentObjectId = "table:customers",
                Facets =
                [
                    new MigrationCatalogFacet { Name = "logicalType", Value = "unsignedInteger" },
                    new MigrationCatalogFacet { Name = "nullable", Value = "false" },
                    new MigrationCatalogFacet { Name = "precision", Value = "20" },
                ],
            },
        ],
        Diagnostics =
        [
            new MigrationDiagnostic
            {
                DiagnosticId = "diag:unsigned-id",
                RuleId = "MIG-TYPE-0001",
                Severity = MigrationDiagnosticSeverity.Warning,
                Status = MigrationCompatibilityStatus.Conditional,
                Evidence = MigrationEvidenceLevel.CapabilityMatched,
                Summary = "Unsigned range requires profiling.",
                Explanation = "Values above Int64.MaxValue require a lossless text representation.",
                ObjectId = "table:customers/column:id",
                Remediation = "Run a full range scan or choose canonical text storage.",
                CanOverride = false,
            },
        ],
    };

    private static MigrationPlan CreatePlan(
        MigrationCatalog catalog,
        MigrationProfileCoverage coverage)
    {
        MigrationCatalogObject column = catalog.Objects.Single(item => item.NativeType is not null);
        var mappingProvider = new StandardDataTypeMappingProvider();
        MigrationTypeMappingDecision decision = mappingProvider.Map(
            new MigrationTypeMappingRequest
            {
                SourceObject = column,
                Profile = MigrationMappingProfile.Preserve,
                Coverage = coverage,
            });

        return new MigrationPlan
        {
            TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            Source = catalog.Source,
            CatalogDigest = MigrationArtifactSerializer.ComputeCatalogDigest(catalog),
            CapabilityDigest = CSharpDbCapabilityCatalogLoader.LoadEmbedded().Digest,
            NamingAlgorithmVersion = DeterministicMigrationNameMapper.AlgorithmVersion,
            MappingPolicyId = mappingProvider.PolicyId,
            MappingPolicyVersion = mappingProvider.PolicyVersion,
            MappingProfile = MigrationMappingProfile.Preserve,
            Objects =
            [
                new MigrationPlanObject
                {
                    SourceObjectId = "table:customers",
                    TargetName = "customers",
                },
                new MigrationPlanObject
                {
                    SourceObjectId = "table:customers/column:id",
                    TargetParentObjectId = "table:customers",
                    TargetName = "id",
                    TypeMappings = [decision.Mapping],
                },
            ],
            Diagnostics = catalog.Diagnostics,
        };
    }

    private static MigrationPlan CreateLossyPlan(MigrationCatalog catalog, bool accepted)
    {
        MigrationCatalogObject column = catalog.Objects.Single(item => item.NativeType is not null);
        MigrationTypeMappingDecision decision = new StandardDataTypeMappingProvider().Map(
            new MigrationTypeMappingRequest
            {
                SourceObject = column,
                Profile = MigrationMappingProfile.Queryable,
                Coverage = new MigrationProfileCoverage
                {
                    Kind = MigrationCoverageKind.Full,
                    ValuesExamined = 10,
                    TotalValues = 10,
                    RequiresFullStreamValidation = false,
                },
            });
        MigrationDiagnostic diagnostic = decision.Diagnostic!;

        return new MigrationPlan
        {
            TargetCSharpDbVersion = catalog.TargetCSharpDbVersion,
            Source = catalog.Source,
            CatalogDigest = MigrationArtifactSerializer.ComputeCatalogDigest(catalog),
            CapabilityDigest = CSharpDbCapabilityCatalogLoader.LoadEmbedded().Digest,
            NamingAlgorithmVersion = DeterministicMigrationNameMapper.AlgorithmVersion,
            MappingPolicyId = new StandardDataTypeMappingProvider().PolicyId,
            MappingPolicyVersion = new StandardDataTypeMappingProvider().PolicyVersion,
            MappingProfile = MigrationMappingProfile.Queryable,
            Objects =
            [
                new MigrationPlanObject
                {
                    SourceObjectId = "table:customers",
                    TargetName = "customers",
                },
                new MigrationPlanObject
                {
                    SourceObjectId = "table:customers/column:id",
                    TargetParentObjectId = "table:customers",
                    TargetName = "id",
                    TypeMappings = [decision.Mapping],
                },
            ],
            Diagnostics = [.. catalog.Diagnostics, diagnostic],
            AcceptedDiagnosticIds = accepted ? [diagnostic.DiagnosticId] : [],
        };
    }

    private static string ReadGolden(string fileName)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "Fixtures", fileName);
        return NormalizeLineEndings(File.ReadAllText(path));
    }

    private static string NormalizeLineEndings(string value) =>
        value.Replace("\r\n", "\n", StringComparison.Ordinal).TrimEnd() + "\n";

    private static string Redigest(JsonObject envelope)
    {
        string format = envelope["format"]!.GetValue<string>();
        string digestAlgorithm = envelope["digestAlgorithm"]!.GetValue<string>();
        JsonNode payload = envelope["payload"]!;
        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(
            new { Format = format, DigestAlgorithm = digestAlgorithm, Payload = payload },
            DigestOptions);
        envelope["digest"] = Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
        return envelope.ToJsonString(new JsonSerializerOptions(DigestOptions) { WriteIndented = true });
    }

    private static JsonSerializerOptions CreateDigestOptions()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };
        options.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase, allowIntegerValues: false));
        return options;
    }

    private sealed class DelegatingMappingPolicy : IDataTypeMappingProvider
    {
        private readonly StandardDataTypeMappingProvider _inner = new();

        public string PolicyId => "test-delegating-mapping";

        public int PolicyVersion => 7;

        public MigrationTypeMappingDecision Map(MigrationTypeMappingRequest request) =>
            _inner.Map(request);
    }
}
