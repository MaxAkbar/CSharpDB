using System.Reflection;
using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonTypedTableSchemaIntegrationTests
{
    private const string TypedValueContract =
        "csharpdb-json-typed-value/v1";
    private const string TextCodecContract =
        "csharpdb-text-codec/v1";

    [Fact]
    public async Task AllCodecsBindExplicitSchemaWhileSparseColumnsStayOrdinary()
    {
        await using TypedSource origin = await TypedSource.CreateAsync(
            AllCodecJson,
            AllCodecOptions());

        JsonTypedTableSchemaInferenceResult result = origin.Schema;
        Assert.Equal(
            "csharpdb-json-typed-table-schema-v1",
            JsonTypedTableSchemaInferenceResult.AlgorithmId);
        Assert.Equal(
            "csharpdb-json-typed-table-scalar-v1",
            JsonTypedTableSchemaInferenceResult.ScalarPolicyId);
        Assert.Equal(
            origin.Intent.ManifestDigest,
            result.IntentManifest.ManifestDigest);
        Assert.Equal(1, result.TotalRecords);
        Assert.Equal(1, result.EligibleObjectRecords);
        Assert.Equal(MigrationCoverageKind.Full, result.StructuralCoverage.Kind);
        Assert.Equal(MigrationCoverageKind.Full, result.TypeProfileCoverage.Kind);
        Assert.Equal(11, result.Columns.Count);

        MigrationSourceValueKind[] expectedKinds =
        [
            MigrationSourceValueKind.Binary,
            MigrationSourceValueKind.Decimal,
            MigrationSourceValueKind.Decimal,
            MigrationSourceValueKind.Guid,
            MigrationSourceValueKind.Date,
            MigrationSourceValueKind.Time,
            MigrationSourceValueKind.DateTime,
            MigrationSourceValueKind.DateTimeOffset,
            MigrationSourceValueKind.SignedInteger,
            MigrationSourceValueKind.UnsignedInteger,
        ];
        JsonTypedValueCodec[] expectedCodecs =
            Enum.GetValues<JsonTypedValueCodec>();

        for (int index = 0; index < expectedCodecs.Length; index++)
        {
            JsonTypedTableColumnSchema column = result.Columns[index];
            JsonTypedColumnIntent intent =
                Assert.IsType<JsonTypedColumnIntent>(column.Intent);
            Assert.Equal(index, column.RepresentationSchema.ColumnIndex);
            Assert.Equal(expectedCodecs[index], intent.Codec);
            Assert.Equal(expectedKinds[index], column.SourceValueKind);
            Assert.Equal(
                JsonTableColumnSchemaResolution.ExplicitOverride,
                column.RepresentationSchema.Resolution);
            Assert.Equal(
                JsonTableInferenceConfidence.Explicit,
                column.RepresentationSchema.Confidence);
            Assert.True(
                column.RequiresFullStreamValidation);
        }

        JsonTypedTableColumnSchema ordinary = result.Columns[10];
        Assert.Null(ordinary.Intent);
        Assert.Equal(
            JsonTableColumnLogicalType.Text,
            ordinary.RepresentationSchema.LogicalType);
        Assert.Equal(
            MigrationSourceValueKind.Text,
            ordinary.SourceValueKind);
        Assert.False(
            ordinary.RequiresFullStreamValidation);
    }

    [Fact]
    public async Task ExactNameGuardAndUserOverridesCannotRetargetIntent()
    {
        var wrongName = new JsonTypedIntentOptions
        {
            Columns =
            [
                Intent(
                    0,
                    "Name",
                    JsonTypedValueCodec.Int64String),
            ],
        };
        JsonTypedTableSchemaException nameError =
            await Assert.ThrowsAsync<JsonTypedTableSchemaException>(
            async () =>
            {
                await using TypedSource _ = await TypedSource.CreateAsync(
                    """[{"name":"1"}]""",
                    wrongName);
            });
        Assert.Equal(
            JsonTypedTableSchemaRules.ColumnMismatch,
            nameError.RuleId);

        var overlappingOverride = new JsonTableSchemaInferenceOptions
        {
            ColumnOverrides =
            [
                new JsonTableColumnSchemaOverride
                {
                    ColumnIndex = 0,
                    ExpectedPropertyName = "value",
                    LogicalType =
                        JsonTableColumnLogicalType.Text,
                },
            ],
        };
        await Assert.ThrowsAsync<ArgumentException>(
            async () =>
            {
                await using TypedSource _ = await TypedSource.CreateAsync(
                    """[{"value":"1"}]""",
                    OneCodecOptions(
                        JsonTypedValueCodec.Int64String),
                    inferenceOptions: overlappingOverride);
            });
    }

    [Fact]
    public async Task ExactDecodedUnicodeNamesRemainOrdinalAndCaseSensitive()
    {
        const string json =
            """[{"é":"1","e\u0301":"2","Name":"3","name":"4","":"5"}]""";
        var options = new JsonTypedIntentOptions
        {
            Columns =
            [
                Intent(0, "é", JsonTypedValueCodec.Int64String),
                Intent(1, "e\u0301", JsonTypedValueCodec.Int64String),
                Intent(2, "Name", JsonTypedValueCodec.Int64String),
                Intent(3, "name", JsonTypedValueCodec.Int64String),
                Intent(4, "", JsonTypedValueCodec.Int64String),
            ],
        };

        await using TypedSource origin =
            await TypedSource.CreateAsync(json, options);

        Assert.Equal(
            ["é", "e\u0301", "Name", "name", ""],
            origin.Schema.Columns.Select(
                item => item.RepresentationSchema.OriginalPropertyName));
        Assert.All(
            origin.Schema.Columns,
            item => Assert.NotNull(item.Intent));
    }

    [Fact]
    public async Task IntentCannotDeclareAColumnOutsideTheDiscoveredShape()
    {
        var options = new JsonTypedIntentOptions
        {
            Columns =
            [
                Intent(
                    1,
                    "missing",
                    JsonTypedValueCodec.Int64String),
            ],
        };

        JsonTypedTableSchemaException error =
            await Assert.ThrowsAsync<JsonTypedTableSchemaException>(
            async () =>
            {
                await using TypedSource _ = await TypedSource.CreateAsync(
                    """[{"present":"1"}]""",
                    options);
            });
        Assert.Equal(
            JsonTypedTableSchemaRules.ColumnMismatch,
            error.RuleId);
    }

    [Fact]
    public async Task NullabilityAndMissingAsNullRemainExplicitSchemaFacts()
    {
        await using TypedSource inferredNullable =
            await TypedSource.CreateAsync(
                """[{"value":null},{"value":"AA=="}]""",
                OneCodecOptions(
                    JsonTypedValueCodec.BinaryBase64));
        JsonTypedTableColumnSchema inferred =
            Assert.Single(inferredNullable.Schema.Columns);
        Assert.True(inferred.RepresentationSchema.Nullable);
        Assert.Equal(
            JsonMissingPropertyPolicy.Reject,
            inferred.RepresentationSchema.MissingPolicy);

        await using TypedSource missingAsNull =
            await TypedSource.CreateAsync(
                """[{"value":"AA=="},{}]""",
                OneCodecOptions(
                    JsonTypedValueCodec.BinaryBase64,
                    nullable: true,
                    missingPolicy:
                        JsonMissingPropertyPolicy.AsNull));
        JsonTypedTableColumnSchema rewritten =
            Assert.Single(missingAsNull.Schema.Columns);
        Assert.True(rewritten.RepresentationSchema.Nullable);
        Assert.Equal(
            JsonMissingPropertyPolicy.AsNull,
            rewritten.RepresentationSchema.MissingPolicy);
    }

    [Fact]
    public async Task ManifestFromAnotherBindingFailsBeforeTypedSchemaIsReturned()
    {
        await using BoundSource first =
            await BoundSource.CreateAsync("""[{"value":"1"}]""");
        JsonTypedIntentManifest intent =
            await first.WriteIntentAsync(
                OneCodecOptions(
                    JsonTypedValueCodec.Int64String));
        await using BoundSource second =
            await BoundSource.CreateAsync("""[{"value":"2"}]""");

        JsonTypedIntentException error =
            await Assert.ThrowsAsync<JsonTypedIntentException>(
            async () => await JsonTypedTableSchemaInferer.InferAsync(
                second.Binding,
                second.Snapshot,
                intent,
                maxProfileRecords: 10,
                cancellationToken: Cancellation));
        Assert.Equal(
            JsonTypedIntentRules.SourceMismatch,
            error.RuleId);
    }

    [Fact]
    public async Task TypedCatalogFreezesIntentContractsLimitsAndNativeTypes()
    {
        await using TypedSource origin = await TypedSource.CreateAsync(
            AllCodecJson,
            AllCodecOptions());
        MigrationCatalog catalog = origin.Schema.CreateCatalog(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);
        MigrationContractValidator.ValidateCatalog(catalog);

        MigrationCatalogObject table = Object(
            catalog,
            JsonMigrationObjectIds.Table);
        Assert.Equal(
            JsonTypedTableSchemaInferenceResult.AlgorithmId,
            Facet(table, "jsonSchemaAlgorithm"));
        Assert.Equal(
            JsonTypedTableSchemaInferenceResult.ScalarPolicyId,
            Facet(table, "jsonScalarPolicy"));
        Assert.Equal(
            JsonTypedIntentSidecar.Format,
            Facet(table, "jsonTypedIntentFormat"));
        Assert.Equal(
            origin.Intent.ManifestDigest,
            Facet(table, "jsonTypedIntentManifestDigest"));
        Assert.Equal(
            TypedValueContract,
            Facet(table, "jsonTypedValueContract"));
        Assert.Equal(
            TextCodecContract,
            Facet(table, "jsonTextCodecContract"));
        Assert.Equal(
            "4096",
            Facet(table, "jsonMaxDecodedBinaryBytes"));
        Assert.Equal(
            "100",
            Facet(table, "jsonMaxDecimalDigits"));

        string[] expectedNativeTypes =
        [
            "JSON_BASE64_STRING",
            "JSON_DECIMAL_STRING",
            "JSON_DECIMAL_NUMBER",
            "JSON_GUID_D_STRING",
            "JSON_DATE_CSHARPDB_TEXT",
            "JSON_TIME_CSHARPDB_TEXT",
            "JSON_DATETIME_CSHARPDB_TEXT",
            "JSON_DATETIMEOFFSET_CSHARPDB_TEXT",
            "JSON_INT64_STRING",
            "JSON_UINT64_STRING",
        ];
        string[] expectedCodecNames =
        [
            "binaryBase64",
            "decimalString",
            "decimalNumber",
            "guidD",
            "dateCSharpDbText",
            "timeCSharpDbText",
            "dateTimeCSharpDbText",
            "dateTimeOffsetCSharpDbText",
            "int64String",
            "uint64String",
        ];
        for (int index = 0; index < expectedNativeTypes.Length; index++)
        {
            MigrationCatalogObject column = Column(catalog, index);
            Assert.Equal(expectedNativeTypes[index], column.NativeType);
            Assert.Equal(
                origin.Intent.ManifestDigest,
                Facet(column, "jsonTypedIntentManifestDigest"));
            Assert.Equal(
                expectedCodecNames[index],
                Facet(column, "jsonTypedCodec"));
            Assert.Equal(
                TypedValueContract,
                Facet(column, "jsonTypedValueContract"));
            Assert.Equal(
                "full-stream",
                Facet(column, "jsonTypedValidation"));
            Assert.Equal(
                index == 2 ? "number" : "string",
                Facet(column, "jsonTypedJsonKind"));
        }

        Assert.Equal("38", Facet(Column(catalog, 1), "precision"));
        Assert.Equal("18", Facet(Column(catalog, 1), "scale"));
        Assert.Equal("10", Facet(Column(catalog, 2), "precision"));
        Assert.Equal("2", Facet(Column(catalog, 2), "scale"));
        for (int index = 3; index <= 7; index++)
        {
            Assert.Equal(
                TextCodecContract,
                Facet(
                    Column(catalog, index),
                    "jsonTextCodecContract"));
        }
        Assert.DoesNotContain(
            Column(catalog, 10).Facets,
            facet => facet.Name.StartsWith(
                "jsonTyped",
                StringComparison.Ordinal));

        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        Assert.Equal(DbType.Blob, Mapping(plan, 0).TargetType);
        Assert.Equal(DbType.Text, Mapping(plan, 1).TargetType);
        Assert.Equal(
            "json-typed-decimal-text",
            Mapping(plan, 1).Conversion?.ConversionId);
        Assert.Equal(DbType.Decimal, Mapping(plan, 2).TargetType);
        Assert.Equal(
            "decimal-native",
            Mapping(plan, 2).Conversion?.ConversionId);
        Assert.Equal(DbType.Text, Mapping(plan, 3).TargetType);
        Assert.Equal(DbType.Integer, Mapping(plan, 8).TargetType);
        Assert.Equal(DbType.Text, Mapping(plan, 9).TargetType);
    }

    [Fact]
    public async Task AnyIntentPolicyChangeChangesCatalogDigest()
    {
        await using TypedSource required = await TypedSource.CreateAsync(
            """[{"value":"1"}]""",
            OneCodecOptions(
                JsonTypedValueCodec.Int64String,
                nullable: false));
        await using TypedSource nullable = await TypedSource.CreateAsync(
            """[{"value":"1"}]""",
            OneCodecOptions(
                JsonTypedValueCodec.Int64String,
                nullable: true));

        MigrationCatalog first = required.Schema.CreateCatalog(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);
        MigrationCatalog second = nullable.Schema.CreateCatalog(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);

        Assert.NotEqual(
            required.Intent.ManifestDigest,
            nullable.Intent.ManifestDigest);
        Assert.NotEqual(
            MigrationArtifactSerializer.ComputeCatalogDigest(first),
            MigrationArtifactSerializer.ComputeCatalogDigest(second));
    }

    [Fact]
    public void RetainedPackageV1HasNoTypedSchemaOverload()
    {
        MethodInfo[] methods = typeof(JsonSnapshotPackage)
            .GetMethods(BindingFlags.Public | BindingFlags.Static);

        Assert.DoesNotContain(
            methods,
            method => method.Name == nameof(JsonSnapshotPackage.WriteAsync) &&
                      method.GetParameters().Any(parameter =>
                          parameter.ParameterType ==
                          typeof(JsonTypedTableSchemaInferenceResult)));
        Assert.Contains(
            methods,
            method => method.Name == nameof(JsonSnapshotPackage.WriteAsync) &&
                      method.GetParameters().Any(parameter =>
                          parameter.ParameterType ==
                          typeof(JsonTableSchemaInferenceResult)));
    }

    [Fact]
    public async Task OrdinaryV1InferenceAndCatalogRemainUntyped()
    {
        await using BoundSource source =
            await BoundSource.CreateAsync(
                """[{"value":"00112233-4455-6677-8899-aabbccddeeff"}]""");
        JsonTableSchemaInferenceResult ordinary =
            await JsonTableSchemaInferer.InferAsync(
                source.Binding,
                source.Snapshot,
                maxProfileRecords: 10,
                cancellationToken: Cancellation);
        MigrationCatalog catalog = ordinary.CreateCatalog(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);

        Assert.Equal(
            "csharpdb-json-table-schema-v1",
            JsonTableSchemaInferenceResult.AlgorithmId);
        Assert.Equal(
            JsonTableColumnLogicalType.Text,
            Assert.Single(ordinary.Columns).LogicalType);
        Assert.DoesNotContain(
            Object(catalog, JsonMigrationObjectIds.Table).Facets,
            facet => facet.Name.StartsWith(
                "jsonTyped",
                StringComparison.Ordinal));
    }

    private static JsonTypedIntentOptions AllCodecOptions() => new()
    {
        MaxDecodedBinaryBytes = 4096,
        MaxDecimalDigits = 100,
        Columns =
        [
            Intent(0, "binary", JsonTypedValueCodec.BinaryBase64),
            Intent(
                1,
                "decimalString",
                JsonTypedValueCodec.DecimalString,
                precision: 38,
                scale: 18),
            Intent(
                2,
                "decimalNumber",
                JsonTypedValueCodec.DecimalNumber,
                precision: 10,
                scale: 2),
            Intent(3, "guid", JsonTypedValueCodec.GuidD),
            Intent(4, "date", JsonTypedValueCodec.DateCSharpDbText),
            Intent(5, "time", JsonTypedValueCodec.TimeCSharpDbText),
            Intent(
                6,
                "dateTime",
                JsonTypedValueCodec.DateTimeCSharpDbText),
            Intent(
                7,
                "dateTimeOffset",
                JsonTypedValueCodec.DateTimeOffsetCSharpDbText),
            Intent(8, "int64", JsonTypedValueCodec.Int64String),
            Intent(9, "uint64", JsonTypedValueCodec.UInt64String),
        ],
    };

    private static JsonTypedIntentOptions OneCodecOptions(
        JsonTypedValueCodec codec,
        bool? nullable = null,
        JsonMissingPropertyPolicy missingPolicy =
            JsonMissingPropertyPolicy.Reject,
        int precision = 38,
        int scale = 18,
        int maxDecodedBinaryBytes = 4096,
        int maxDecimalDigits = 100) => new()
        {
            MaxDecodedBinaryBytes = maxDecodedBinaryBytes,
            MaxDecimalDigits = maxDecimalDigits,
            Columns =
            [
                Intent(
                    0,
                    "value",
                    codec,
                    nullable,
                    missingPolicy,
                    codec is
                        JsonTypedValueCodec.DecimalString or
                        JsonTypedValueCodec.DecimalNumber
                        ? precision
                        : null,
                    codec is
                        JsonTypedValueCodec.DecimalString or
                        JsonTypedValueCodec.DecimalNumber
                        ? scale
                        : null),
            ],
        };

    private static JsonTypedColumnIntent Intent(
        int index,
        string name,
        JsonTypedValueCodec codec,
        bool? nullable = null,
        JsonMissingPropertyPolicy missingPolicy =
            JsonMissingPropertyPolicy.Reject,
        int? precision = null,
        int? scale = null) => new()
        {
            ColumnIndex = index,
            ExpectedPropertyName = name,
            Codec = codec,
            Nullable = nullable,
            MissingPolicy = missingPolicy,
            Precision = precision,
            Scale = scale,
        };

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
        item.Facets.Single(
            facet => string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal)).Value;

    private static MigrationTypeMapping Mapping(
        MigrationPlan plan,
        int columnIndex) =>
        Assert.Single(
            Assert.Single(
                plan.Objects,
                item => item.SourceObjectId ==
                        JsonMigrationObjectIds.Column(columnIndex))
                .TypeMappings);

    private const string AllCodecJson =
        """
        [
          {
            "binary":"AQIDBA==",
            "decimalString":"12345678901234567890.123456789012345678",
            "decimalNumber":123.45,
            "guid":"00112233-4455-6677-8899-aabbccddeeff",
            "date":"2024-02-29",
            "time":"08:09:10.1234567",
            "dateTime":"2026-07-23 08:09:10.1234567",
            "dateTimeOffset":"2026-07-23 08:09:10.1234567-07:00",
            "int64":"-9223372036854775808",
            "uint64":"18446744073709551615",
            "ordinary":"text"
          }
        ]
        """;

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    private static byte[] Utf8Bytes(string value) =>
        new UTF8Encoding(false, true).GetBytes(value);

    private sealed class TypedSource : IAsyncDisposable
    {
        private readonly BoundSource source;

        private TypedSource(
            BoundSource source,
            JsonTypedIntentManifest intent,
            JsonTypedTableSchemaInferenceResult schema)
        {
            this.source = source;
            Intent = intent;
            Schema = schema;
        }

        internal JsonSourceSnapshot Snapshot => source.Snapshot;

        internal JsonSourceBinding Binding => source.Binding;

        internal JsonTypedIntentManifest Intent { get; }

        internal JsonTypedTableSchemaInferenceResult Schema { get; }

        internal static async Task<TypedSource> CreateAsync(
            string json,
            JsonTypedIntentOptions intentOptions,
            JsonTableSchemaInferenceOptions? inferenceOptions = null)
        {
            BoundSource source = await BoundSource.CreateAsync(json);
            try
            {
                JsonTypedIntentManifest intent =
                    await source.WriteIntentAsync(intentOptions);
                JsonTypedTableSchemaInferenceResult schema =
                    await JsonTypedTableSchemaInferer.InferAsync(
                        source.Binding,
                        source.Snapshot,
                        intent,
                        maxProfileRecords: 100,
                        inferenceOptions,
                        Cancellation);
                return new TypedSource(source, intent, schema);
            }
            catch
            {
                await source.DisposeAsync();
                throw;
            }
        }

        public ValueTask DisposeAsync() => source.DisposeAsync();
    }

    private sealed class BoundSource : IAsyncDisposable
    {
        private readonly TemporaryDirectory directory;
        private int sidecarOrdinal;

        private BoundSource(
            TemporaryDirectory directory,
            JsonSourceSnapshot snapshot,
            JsonSourceBinding binding)
        {
            this.directory = directory;
            Snapshot = snapshot;
            Binding = binding;
        }

        internal JsonSourceSnapshot Snapshot { get; }

        internal JsonSourceBinding Binding { get; }

        internal static async Task<BoundSource> CreateAsync(
            string json)
        {
            var directory = new TemporaryDirectory();
            JsonSourceSnapshot? snapshot = null;
            try
            {
                snapshot = await JsonSourceSnapshot.CreateAsync(
                    new MemoryStream(Utf8Bytes(json)),
                    new JsonSourceSnapshotOptions
                    {
                        WorkspacePath = directory.Root,
                        MaxSourceBytes = 4 * 1024 * 1024,
                    },
                    Cancellation);
                JsonSourceBinding binding =
                    await JsonSourceBinding.CreateAsync(
                        snapshot,
                        cancellationToken: Cancellation);
                return new BoundSource(
                    directory,
                    snapshot,
                    binding);
            }
            catch
            {
                if (snapshot is not null)
                    await snapshot.DisposeAsync();
                directory.Dispose();
                throw;
            }
        }

        internal async Task<JsonTypedIntentManifest> WriteIntentAsync(
            JsonTypedIntentOptions options)
        {
            int ordinal = Interlocked.Increment(
                ref sidecarOrdinal);
            string path = Path.Combine(
                directory.Root,
                $"intent-{ordinal}{JsonTypedIntentSidecar.FileExtension}");
            return await JsonTypedIntentSidecar.WriteAsync(
                path,
                Binding,
                options,
                Cancellation);
        }

        public async ValueTask DisposeAsync()
        {
            await Snapshot.DisposeAsync();
            directory.Dispose();
        }
    }

    private sealed class TemporaryDirectory : IDisposable
    {
        internal TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                "csharpdb-json-typed-schema-tests-" +
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        public void Dispose()
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
    }
}
