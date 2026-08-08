using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonMigrationCatalogTests
{
    private static string CurrentVersion =>
        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion;

    [Fact]
    public async Task CatalogRetainsOrderedSchemaTypesCoverageAndSourceBinding()
    {
        const string json =
            """
            [
              {
                "text":"alpha",
                "flag":true,
                "signed":-1,
                "unsigned":18446744073709551615,
                "amount":1.25,
                "nested":{"z":1}
              },
              {
                "text":"beta",
                "flag":false,
                "signed":2,
                "unsigned":9223372036854775808,
                "amount":2.5,
                "nested":[1,2]
              }
            ]
            """;
        (JsonSourceSnapshot snapshot, JsonSourceBinding binding) =
            await BoundSourceAsync(json);
        await using (snapshot)
        {
            JsonTableSchemaInferenceResult result =
                await JsonTableSchemaInferer.InferAsync(
                    binding,
                    snapshot,
                    maxProfileRecords: 100,
                    cancellationToken:
                        TestContext.Current.CancellationToken);

            MigrationCatalog catalog =
                JsonMigrationCatalogBuilder.Build(
                    result,
                    CurrentVersion);

            MigrationContractValidator.ValidateCatalog(catalog);
            Assert.Equal(CurrentVersion, catalog.TargetCSharpDbVersion);
            Assert.Equal(binding.Source, catalog.Source);
            Assert.Equal(
                [JsonMigrationObjectIds.MainNamespace,
                 JsonMigrationObjectIds.Table,
                 .. Enumerable.Range(0, 6)
                     .Select(JsonMigrationObjectIds.Column)],
                catalog.Objects.Select(item => item.ObjectId));
            Assert.Equal(
                ["text", "flag", "signed", "unsigned", "amount", "nested"],
                result.Columns.Select(column => column.SourceName));
            Assert.Equal(
                Enumerable.Range(0, 6),
                result.Columns.Select(column => column.ColumnIndex));

            MigrationCatalogObject table = Object(
                catalog,
                JsonMigrationObjectIds.Table);
            Assert.Equal("json_data", table.SourceName);
            Assert.Equal(
                JsonTableSchemaInferenceResult.AlgorithmId,
                Facet(table, "jsonSchemaAlgorithm"));
            Assert.Equal(
                JsonTableSchemaInferenceResult.ScalarPolicyId,
                Facet(table, "jsonScalarPolicy"));
            Assert.Equal(
                binding.OptionsDigest,
                Facet(table, "jsonSourceBindingOptionsDigest"));
            Assert.Equal(
                binding.SnapshotIdentity,
                Facet(table, "jsonSnapshotIdentity"));
            Assert.Equal("root-array", Facet(table, "jsonInputFraming"));
            Assert.Equal(
                "Full",
                Facet(table, "jsonStructuralCoverageKind"));
            Assert.Equal(
                "Full",
                Facet(table, "jsonTypeProfileCoverageKind"));
            Assert.Equal("2", Facet(table, "jsonTotalRecords"));

            AssertColumn(
                catalog,
                0,
                "text",
                "JSON_STRING",
                "text");
            AssertColumn(
                catalog,
                1,
                "flag",
                "JSON_BOOLEAN",
                "boolean");
            AssertColumn(
                catalog,
                2,
                "signed",
                "JSON_SIGNED_INTEGER",
                "signedInteger");
            AssertColumn(
                catalog,
                3,
                "unsigned",
                "JSON_UNSIGNED_INTEGER",
                "unsignedInteger");
            AssertColumn(
                catalog,
                4,
                "amount",
                "JSON_DECIMAL",
                "decimal");
            AssertColumn(
                catalog,
                5,
                "nested",
                "JSON_CANONICAL",
                "text");

            MigrationCatalogObject amount = Column(catalog, 4);
            Assert.Equal("3", Facet(amount, "precision"));
            Assert.Equal("2", Facet(amount, "scale"));
            Assert.Equal("1", Facet(amount, "jsonFirstSeenRecordOrdinal"));
            Assert.Equal("4", Facet(amount, "jsonFirstSeenPropertyOrdinal"));
            Assert.Equal("amount", Facet(
                amount,
                "jsonOriginalPropertyName"));
            Assert.Equal(
                result.Diagnostics.Select(item => item.DiagnosticId),
                catalog.Diagnostics.Select(item => item.DiagnosticId));

            MigrationPlan plan =
                new MigrationPlanner().CreatePlan(catalog);
            AssertMapping(
                plan,
                0,
                DbType.Text,
                MigrationMappingClassification.Exact);
            AssertMapping(
                plan,
                1,
                DbType.Integer,
                MigrationMappingClassification.LosslessReencoded);
            AssertMapping(
                plan,
                2,
                DbType.Integer,
                MigrationMappingClassification.Exact);
            AssertMapping(
                plan,
                3,
                DbType.Text,
                MigrationMappingClassification.LosslessReencoded);
            AssertMapping(
                plan,
                4,
                DbType.Decimal,
                MigrationMappingClassification.LosslessReencoded);
            AssertMapping(
                plan,
                5,
                DbType.Text,
                MigrationMappingClassification.Exact);
            MigrationTypeMapping fullMapping = Mapping(
                plan,
                JsonMigrationObjectIds.Column(0));
            Assert.Equal(
                MigrationCoverageKind.Full,
                fullMapping.Coverage.Kind);
            Assert.Equal(2, fullMapping.Coverage.ValuesExamined);
            Assert.Equal(2, fullMapping.Coverage.TotalValues);
            Assert.False(
                fullMapping.Coverage.RequiresFullStreamValidation);
        }
    }

    [Fact]
    public async Task CanonicalJsonColumnUsesExactTextMappingAndVersionFacets()
    {
        const string json =
            """[{"payload":{"b":2,"a":1}},{"payload":["x",1e400,-0]}]""";
        (JsonSourceSnapshot snapshot, JsonSourceBinding binding) =
            await BoundSourceAsync(json);
        await using (snapshot)
        {
            JsonTableSchemaInferenceResult result =
                await JsonTableSchemaInferer.InferAsync(
                    binding,
                    snapshot,
                    100,
                    cancellationToken:
                        TestContext.Current.CancellationToken);
            MigrationCatalog catalog =
                JsonMigrationCatalogBuilder.Build(
                    result,
                    CurrentVersion);
            MigrationCatalogObject payload = Column(catalog, 0);

            Assert.Equal("JSON_CANONICAL", payload.NativeType);
            Assert.Equal("text", Facet(payload, "logicalType"));
            Assert.Equal("json", Facet(
                payload,
                "jsonTableLogicalType"));
            Assert.Equal(
                "canonical-json-text",
                Facet(payload, "jsonRepresentation"));
            Assert.Equal(
                JsonInputContracts.CanonicalNestedJsonVersion,
                Facet(payload, "jsonCanonicalValueVersion"));
            Assert.Equal(
                JsonInputContracts.PropertyOrderPolicy,
                Facet(payload, "jsonPropertyOrderPolicy"));
            Assert.Equal(
                JsonInputContracts.NumberLexemePolicy,
                Facet(payload, "jsonNumberLexemePolicy"));

            MigrationPlan plan =
                new MigrationPlanner().CreatePlan(catalog);
            MigrationTypeMapping mapping =
                Mapping(plan, JsonMigrationObjectIds.Column(0));
            Assert.Equal(DbType.Text, mapping.TargetType);
            Assert.Equal(
                MigrationMappingClassification.Exact,
                mapping.Classification);
            Assert.Null(mapping.Conversion);
        }
    }

    [Fact]
    public async Task CatalogPreservesBlankOriginalNameAndOrdinalFallback()
    {
        const string json = """[{"":1,"Name":"x","name":"y"}]""";
        (JsonSourceSnapshot snapshot, JsonSourceBinding binding) =
            await BoundSourceAsync(json);
        await using (snapshot)
        {
            JsonTableSchemaInferenceResult result =
                await JsonTableSchemaInferer.InferAsync(
                    binding,
                    snapshot,
                    100,
                    cancellationToken:
                        TestContext.Current.CancellationToken);
            MigrationCatalog catalog =
                JsonMigrationCatalogBuilder.Build(
                    result,
                    CurrentVersion);

            MigrationCatalogObject blank = Column(catalog, 0);
            Assert.Equal("column_1", blank.SourceName);
            Assert.Equal(string.Empty, Facet(
                blank,
                "jsonOriginalPropertyName"));
            Assert.Equal("0", Facet(
                blank,
                "jsonFirstSeenPropertyOrdinal"));
            Assert.Equal("Name", Column(catalog, 1).SourceName);
            Assert.Equal("name", Column(catalog, 2).SourceName);
            Assert.Contains(
                catalog.Diagnostics,
                item =>
                    item.RuleId ==
                        JsonTableSchemaDiagnosticRules.PropertyName &&
                    item.ObjectId ==
                        JsonMigrationObjectIds.Column(0));
        }
    }

    [Fact]
    public async Task ProfiledInspectorFullyDiscoversPropertiesBeyondSample()
    {
        const string json =
            """[{"first":1},{"first":2},{"late":"tail"}]""";
        (JsonSourceSnapshot snapshot, JsonSourceBinding binding) =
            await BoundSourceAsync(json);
        await using (snapshot)
        {
            var inspector = new JsonMigrationSourceInspector(
                binding,
                snapshot);

            MigrationCatalog catalog = await inspector.InspectAsync(
                new MigrationInspectionRequest
                {
                    TargetCSharpDbVersion = CurrentVersion,
                    IncludeProfile = true,
                    ProfileSampleSize = 1,
                },
                TestContext.Current.CancellationToken);

            Assert.Equal(MigrationSourceKind.Json, inspector.SourceKind);
            Assert.Equal(binding.Source, catalog.Source);
            Assert.Equal(2, catalog.Objects.Count(
                item => item.Kind == MigrationObjectKind.Column));
            Assert.Equal("late", Column(catalog, 1).SourceName);
            Assert.Equal(
                "Full",
                Facet(
                    Object(catalog, JsonMigrationObjectIds.Table),
                    "jsonStructuralCoverageKind"));
            Assert.Equal(
                "Sample",
                Facet(
                    Object(catalog, JsonMigrationObjectIds.Table),
                    "jsonTypeProfileCoverageKind"));
            Assert.Equal(
                "1",
                Facet(
                    Object(catalog, JsonMigrationObjectIds.Table),
                    "jsonProfileRecordsExamined"));
            Assert.Equal(
                "JSON_CANONICAL",
                Column(catalog, 1).NativeType);
            MigrationTypeMapping sampledMapping = Mapping(
                new MigrationPlanner().CreatePlan(catalog),
                JsonMigrationObjectIds.Column(0));
            Assert.Equal(
                MigrationCoverageKind.Sample,
                sampledMapping.Coverage.Kind);
            Assert.Equal(1, sampledMapping.Coverage.ValuesExamined);
            Assert.Equal(3, sampledMapping.Coverage.TotalValues);
            Assert.True(
                sampledMapping.Coverage.RequiresFullStreamValidation);
        }
    }

    [Fact]
    public async Task DiscoveryOnlyInspectorStillFullyScansStructure()
    {
        const string json =
            "{\"first\":1}\n" +
            "{\"second\":2}\n" +
            "{\"third\":3}";
        (JsonSourceSnapshot snapshot, JsonSourceBinding binding) =
            await BoundSourceAsync(
                json,
                new JsonStreamingReaderOptions
                {
                    Framing = JsonInputFraming.MultipleValues,
                });
        await using (snapshot)
        {
            var inspector = new JsonMigrationSourceInspector(
                binding,
                snapshot);

            MigrationCatalog catalog = await inspector.InspectAsync(
                new MigrationInspectionRequest
                {
                    TargetCSharpDbVersion = CurrentVersion,
                    IncludeProfile = false,
                    ProfileSampleSize = 1,
                },
                TestContext.Current.CancellationToken);

            Assert.Equal(
                ["first", "second", "third"],
                catalog.Objects
                    .Where(item =>
                        item.Kind == MigrationObjectKind.Column)
                    .OrderBy(item => int.Parse(
                        Facet(item, "jsonColumnIndex")!,
                        System.Globalization.CultureInfo.InvariantCulture))
                    .Select(item => item.SourceName));
            MigrationCatalogObject table = Object(
                catalog,
                JsonMigrationObjectIds.Table);
            Assert.Equal(
                "multiple-values",
                Facet(table, "jsonInputFraming"));
            Assert.Equal(
                "Full",
                Facet(table, "jsonStructuralCoverageKind"));
            Assert.Equal(
                "None",
                Facet(table, "jsonTypeProfileCoverageKind"));
            Assert.Equal(
                "0",
                Facet(table, "jsonProfileRecordsExamined"));
            Assert.All(
                catalog.Objects.Where(item =>
                    item.Kind == MigrationObjectKind.Column),
                column =>
                    Assert.Equal("JSON_CANONICAL", column.NativeType));
            MigrationTypeMapping unprofiledMapping = Mapping(
                new MigrationPlanner().CreatePlan(catalog),
                JsonMigrationObjectIds.Column(0));
            Assert.Equal(
                MigrationCoverageKind.None,
                unprofiledMapping.Coverage.Kind);
            Assert.Equal(0, unprofiledMapping.Coverage.ValuesExamined);
            Assert.Null(unprofiledMapping.Coverage.TotalValues);
            Assert.True(
                unprofiledMapping.Coverage.RequiresFullStreamValidation);
        }
    }

    [Fact]
    public async Task InspectorRejectsSnapshotAndTargetMismatches()
    {
        (JsonSourceSnapshot first, JsonSourceBinding binding) =
            await BoundSourceAsync("[{\"x\":1}]");
        await using (first)
        await using (JsonSourceSnapshot second =
                     await SnapshotAsync("[{\"x\":2}]"))
        {
            ArgumentException mismatch =
                Assert.Throws<ArgumentException>(
                    () => new JsonMigrationSourceInspector(
                        binding,
                        second));
            Assert.Equal("snapshot", mismatch.ParamName);

            var inspector = new JsonMigrationSourceInspector(
                binding,
                first);
            await Assert.ThrowsAsync<NotSupportedException>(
                async () => await inspector.InspectAsync(
                    new MigrationInspectionRequest
                    {
                        TargetCSharpDbVersion = "999.0.0",
                        IncludeProfile = true,
                        ProfileSampleSize = 1,
                    },
                    TestContext.Current.CancellationToken));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                async () => await inspector.InspectAsync(
                    new MigrationInspectionRequest
                    {
                        TargetCSharpDbVersion = CurrentVersion,
                        IncludeProfile = false,
                        ProfileSampleSize = 0,
                    },
                    TestContext.Current.CancellationToken));
        }
    }

    [Fact]
    public async Task InspectorValidatesAndFreezesSchemaOptionsAtConstruction()
    {
        (JsonSourceSnapshot snapshot, JsonSourceBinding binding) =
            await BoundSourceAsync("[{\"x\":1}]");
        await using (snapshot)
        {
            Assert.Throws<ArgumentOutOfRangeException>(
                () => new JsonMigrationSourceInspector(
                    binding,
                    snapshot,
                    new JsonTableSchemaInferenceOptions
                    {
                        MaxColumns = 0,
                    }));
            Assert.Throws<ArgumentException>(
                () => new JsonMigrationSourceInspector(
                    binding,
                    snapshot,
                    new JsonTableSchemaInferenceOptions
                    {
                        ColumnOverrides =
                        [
                            new JsonTableColumnSchemaOverride
                            {
                                ColumnIndex = 0,
                                ExpectedPropertyName = "x",
                                LogicalType =
                                    JsonTableColumnLogicalType.Text,
                                Nullable = false,
                                MissingPolicy =
                                    JsonMissingPropertyPolicy.AsNull,
                            },
                        ],
                    }));

            JsonTableColumnSchemaOverride[] mutable =
            [
                new()
                {
                    ColumnIndex = 0,
                    ExpectedPropertyName = "x",
                    LogicalType =
                        JsonTableColumnLogicalType.SignedInteger,
                },
            ];
            var inspector = new JsonMigrationSourceInspector(
                binding,
                snapshot,
                new JsonTableSchemaInferenceOptions
                {
                    ColumnOverrides = mutable,
                });
            mutable[0] = mutable[0] with
            {
                ExpectedPropertyName = "changed",
            };

            MigrationCatalog catalog = await inspector.InspectAsync(
                new MigrationInspectionRequest
                {
                    TargetCSharpDbVersion = CurrentVersion,
                    IncludeProfile = true,
                    ProfileSampleSize = 1,
                },
                TestContext.Current.CancellationToken);
            Assert.Equal(
                "ExplicitOverride",
                Facet(
                    Column(catalog, 0),
                    "jsonSchemaResolution"));
        }
    }

    [Fact]
    public void JsonObjectIdsAreOrdinalAndCanonical()
    {
        Assert.Equal("json:column:0", JsonMigrationObjectIds.Column(0));
        Assert.Equal(
            "json:column:123",
            JsonMigrationObjectIds.Column(123));
        Assert.True(JsonMigrationObjectIds.TryParseColumn(
            "json:column:123",
            out int parsed));
        Assert.Equal(123, parsed);
        Assert.False(JsonMigrationObjectIds.TryParseColumn(
            "json:column:0123",
            out _));
        Assert.False(JsonMigrationObjectIds.TryParseColumn(
            "json:column:-1",
            out _));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => JsonMigrationObjectIds.Column(-1));
    }

    private static void AssertColumn(
        MigrationCatalog catalog,
        int index,
        string sourceName,
        string nativeType,
        string logicalType)
    {
        MigrationCatalogObject column = Column(catalog, index);
        Assert.Equal(sourceName, column.SourceName);
        Assert.Equal(nativeType, column.NativeType);
        Assert.Equal(logicalType, Facet(column, "logicalType"));
        Assert.Equal(
            index.ToString(
                System.Globalization.CultureInfo.InvariantCulture),
            Facet(column, "jsonColumnIndex"));
        Assert.Equal(sourceName, Facet(
            column,
            "jsonOriginalPropertyName"));
        Assert.Equal(
            JsonTableSchemaInferenceResult.AlgorithmId,
            Facet(column, "jsonSchemaAlgorithm"));
        Assert.Equal(
            JsonTableSchemaInferenceResult.ScalarPolicyId,
            Facet(column, "jsonScalarPolicy"));
    }

    private static MigrationCatalogObject Column(
        MigrationCatalog catalog,
        int index) =>
        Object(catalog, JsonMigrationObjectIds.Column(index));

    private static MigrationCatalogObject Object(
        MigrationCatalog catalog,
        string objectId) =>
        Assert.Single(
            catalog.Objects,
            item => string.Equals(
                item.ObjectId,
                objectId,
                StringComparison.Ordinal));

    private static string? Facet(
        MigrationCatalogObject item,
        string name) =>
        item.Facets
            .Single(facet => string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal))
            .Value;

    private static MigrationTypeMapping Mapping(
        MigrationPlan plan,
        string sourceObjectId) =>
        Assert.Single(
            Assert.Single(
                plan.Objects,
                item => string.Equals(
                    item.SourceObjectId,
                    sourceObjectId,
                    StringComparison.Ordinal))
            .TypeMappings);

    private static void AssertMapping(
        MigrationPlan plan,
        int columnIndex,
        DbType targetType,
        MigrationMappingClassification classification)
    {
        MigrationTypeMapping mapping = Mapping(
            plan,
            JsonMigrationObjectIds.Column(columnIndex));
        Assert.Equal(targetType, mapping.TargetType);
        Assert.Equal(classification, mapping.Classification);
    }

    private static async ValueTask<(
        JsonSourceSnapshot Snapshot,
        JsonSourceBinding Binding)> BoundSourceAsync(
        string json,
        JsonStreamingReaderOptions? options = null)
    {
        JsonSourceSnapshot snapshot = await SnapshotAsync(json);
        try
        {
            JsonSourceBinding binding =
                await JsonSourceBinding.CreateAsync(
                    snapshot,
                    options,
                    cancellationToken:
                        TestContext.Current.CancellationToken);
            return (snapshot, binding);
        }
        catch
        {
            await snapshot.DisposeAsync();
            throw;
        }
    }

    private static ValueTask<JsonSourceSnapshot> SnapshotAsync(
        string json) =>
        JsonSourceSnapshot.CreateAsync(
            new MemoryStream(
                new UTF8Encoding(false, true).GetBytes(json)),
            cancellationToken:
                TestContext.Current.CancellationToken);
}
