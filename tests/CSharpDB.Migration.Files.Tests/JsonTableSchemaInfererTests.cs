using System.Reflection;
using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonTableSchemaInfererTests
{
    [Fact]
    public async Task FullStructuralScanDiscoversLateColumnsInFirstEncounterOrder()
    {
        JsonTableSchemaInferenceResult result = await InferAsync(
            """[{"b":1,"a":null},{"c":"x","b":2},{"a":3}]""",
            maxProfileRecords: 10);

        Assert.Equal(3, result.TotalRecords);
        Assert.Equal(3, result.EligibleObjectRecords);
        Assert.Equal(MigrationCoverageKind.Full, result.StructuralCoverage.Kind);
        Assert.Equal(MigrationCoverageKind.Full, result.TypeProfileCoverage.Kind);
        Assert.Equal(["b", "a", "c"], result.Columns.Select(item => item.OriginalPropertyName));
        Assert.Equal([0, 1, 2], result.Columns.Select(item => item.ColumnIndex));

        JsonTableColumnSchema b = result.Columns[0];
        Assert.Equal(2, b.PresentCount);
        Assert.Equal(1, b.MissingCount);
        Assert.Equal(JsonTableColumnLogicalType.SignedInteger, b.LogicalType);

        JsonTableColumnSchema a = result.Columns[1];
        Assert.Equal(2, a.PresentCount);
        Assert.Equal(1, a.NullCount);
        Assert.Equal(1, a.MissingCount);
        Assert.True(a.Nullable);

        JsonTableColumnSchema c = result.Columns[2];
        Assert.Equal(1, c.PresentCount);
        Assert.Equal(2, c.MissingCount);
        Assert.Equal(JsonTableColumnLogicalType.Text, c.LogicalType);
    }

    [Fact]
    public async Task ExactUnicodeCaseAndBlankPropertyNamesRemainDistinct()
    {
        JsonTableSchemaInferenceResult result = await InferAsync(
            """[{"":1," ":2,"é":3,"e\u0301":4,"Name":5,"name":6,"column_1":7}]""");

        Assert.Equal(
            ["", " ", "é", "e\u0301", "Name", "name", "column_1"],
            result.Columns.Select(item => item.OriginalPropertyName));
        Assert.Equal("column_1", result.Columns[0].SourceName);
        Assert.Equal("column_2", result.Columns[1].SourceName);
        Assert.Equal("é", result.Columns[2].SourceName);
        Assert.Equal("column_1", result.Columns[6].SourceName);
        Assert.Contains(
            result.Diagnostics,
            item => item.RuleId == JsonTableSchemaDiagnosticRules.PropertyName &&
                    item.ObjectId == "json:column:0");
        Assert.Contains(
            result.Diagnostics,
            item => item.RuleId == JsonTableSchemaDiagnosticRules.PropertyName &&
                    item.ObjectId == "json:column:1");
    }

    [Fact]
    public async Task ExplicitNullAndMissingRemainSeparate()
    {
        JsonTableSchemaInferenceResult result = await InferAsync(
            """[{"x":null},{}]""");

        JsonTableColumnSchema column = Assert.Single(result.Columns);
        Assert.Equal(1, column.PresentCount);
        Assert.Equal(1, column.NullCount);
        Assert.Equal(1, column.MissingCount);
        Assert.True(column.Nullable);
        Assert.Equal(JsonMissingPropertyPolicy.Reject, column.MissingPolicy);
        Assert.Equal(JsonTableColumnLogicalType.Json, column.LogicalType);
        Assert.Contains(
            result.Diagnostics,
            item => item.RuleId == JsonTableSchemaDiagnosticRules.MissingProperty);
    }

    [Fact]
    public async Task MissingAsNullIsAlwaysNullableEvenWithoutObservedAbsence()
    {
        JsonTableSchemaInferenceResult result = await InferAsync(
            """[{"x":1},{"x":2}]""",
            options: Options(
                new JsonTableColumnSchemaOverride
                {
                    ColumnIndex = 0,
                    ExpectedPropertyName = "x",
                    LogicalType = JsonTableColumnLogicalType.SignedInteger,
                    MissingPolicy = JsonMissingPropertyPolicy.AsNull,
                }));

        JsonTableColumnSchema column = Assert.Single(result.Columns);
        Assert.True(column.Nullable);
        Assert.Equal(JsonMissingPropertyPolicy.AsNull, column.MissingPolicy);
        Assert.DoesNotContain(
            result.Diagnostics,
            item => item.RuleId == JsonTableSchemaDiagnosticRules.MissingAsNull);
    }

    [Fact]
    public async Task MissingAsNullProducesAnExplicitRewriteDiagnosticWhenUsed()
    {
        JsonTableSchemaInferenceResult result = await InferAsync(
            """[{"x":1},{}]""",
            options: Options(
                new JsonTableColumnSchemaOverride
                {
                    ColumnIndex = 0,
                    ExpectedPropertyName = "x",
                    LogicalType = JsonTableColumnLogicalType.SignedInteger,
                    MissingPolicy = JsonMissingPropertyPolicy.AsNull,
                }));

        JsonTableColumnSchema column = Assert.Single(result.Columns);
        Assert.True(column.Nullable);
        MigrationDiagnostic diagnostic = Assert.Single(
            result.Diagnostics,
            item => item.RuleId == JsonTableSchemaDiagnosticRules.MissingAsNull);
        Assert.Equal(MigrationCompatibilityStatus.CompatibleWithRewrite, diagnostic.Status);
        Assert.Equal(MigrationDiagnosticSeverity.Warning, diagnostic.Severity);
    }

    [Fact]
    public async Task NonObjectRowsAreCountedWithoutHidingEligibleObjects()
    {
        JsonTableSchemaInferenceResult result = await InferAsync(
            """[1,{"x":2},null,[]]""");

        Assert.Equal(4, result.TotalRecords);
        Assert.Equal(1, result.EligibleObjectRecords);
        Assert.Equal(3, result.IneligibleRecords);
        Assert.Single(result.Columns);
        Assert.Contains(
            result.Diagnostics,
            item => item.RuleId == JsonTableSchemaDiagnosticRules.NonObjectRow &&
                    item.ObjectId == "json:table:0");
    }

    [Theory]
    [InlineData("[]")]
    [InlineData("[{},{}]")]
    [InlineData("[null,1]")]
    public async Task SourcesWithoutAnyObjectPropertyHaveABlockingEmptyShape(
        string json)
    {
        JsonTableSchemaInferenceResult result = await InferAsync(json);

        Assert.Empty(result.Columns);
        Assert.Contains(
            result.Diagnostics,
            item => item.RuleId == JsonTableSchemaDiagnosticRules.EmptyTableShape &&
                    item.Severity == MigrationDiagnosticSeverity.Error);
    }

    [Theory]
    [InlineData("-9223372036854775808", JsonTableColumnLogicalType.SignedInteger)]
    [InlineData("9223372036854775807", JsonTableColumnLogicalType.SignedInteger)]
    [InlineData("9223372036854775808", JsonTableColumnLogicalType.UnsignedInteger)]
    [InlineData("18446744073709551615", JsonTableColumnLogicalType.UnsignedInteger)]
    [InlineData("18446744073709551616", JsonTableColumnLogicalType.Decimal)]
    [InlineData("1.25", JsonTableColumnLogicalType.Decimal)]
    [InlineData("-0", JsonTableColumnLogicalType.Json)]
    [InlineData("1.00", JsonTableColumnLogicalType.Json)]
    [InlineData("1e2", JsonTableColumnLogicalType.Json)]
    [InlineData("1E+2", JsonTableColumnLogicalType.Json)]
    public async Task NumbersUseExactPreservationFirstClassification(
        string lexeme,
        JsonTableColumnLogicalType expected)
    {
        JsonTableSchemaInferenceResult result = await InferAsync(
            $"[{{\"n\":{lexeme}}}]");

        JsonTableColumnSchema column = Assert.Single(result.Columns);
        Assert.Equal(expected, column.LogicalType);
        if (expected == JsonTableColumnLogicalType.Json)
        {
            Assert.Equal(
                JsonTableColumnInferenceReason.LexicalPreservation,
                column.Reason);
            Assert.Equal(1, column.ProfiledLexemePreservationCount);
        }
        Assert.Equal(JsonTableInferenceConfidence.High, column.Confidence);
    }

    [Fact]
    public async Task CompatibleIntegerAndFixedDecimalEvidenceSelectsDecimal()
    {
        JsonTableSchemaInferenceResult result = await InferAsync(
            """[{"n":1},{"n":-2},{"n":3.25}]""");

        JsonTableColumnSchema column = Assert.Single(result.Columns);
        Assert.Equal(JsonTableColumnLogicalType.Decimal, column.LogicalType);
        Assert.Equal(3, column.ObservedPrecision);
        Assert.Equal(2, column.ObservedScale);
    }

    [Fact]
    public async Task NestedAndMixedKindsUseOrderedJsonRepresentation()
    {
        JsonTableSchemaInferenceResult nested = await InferAsync(
            """[{"v":{"b":1,"a":[2]}},{"v":[3,4]}]""");
        JsonTableColumnSchema nestedColumn = Assert.Single(nested.Columns);
        Assert.Equal(JsonTableColumnLogicalType.Json, nestedColumn.LogicalType);
        Assert.Equal(JsonTableColumnInferenceReason.MixedKinds, nestedColumn.Reason);

        JsonTableSchemaInferenceResult mixed = await InferAsync(
            """[{"v":"1"},{"v":1},{"v":true}]""");
        JsonTableColumnSchema mixedColumn = Assert.Single(mixed.Columns);
        Assert.Equal(JsonTableColumnLogicalType.Json, mixedColumn.LogicalType);
        Assert.Equal(JsonTableColumnInferenceReason.MixedKinds, mixedColumn.Reason);
        Assert.Equal(JsonTableColumnSchemaResolution.WidenedToJson, mixedColumn.Resolution);
    }

    [Theory]
    [InlineData("\"1\"")]
    [InlineData("\"2026-07-23\"")]
    [InlineData("\"00112233-4455-6677-8899-aabbccddeeff\"")]
    public async Task JsonStringsNeverInferSemanticIntent(string value)
    {
        JsonTableSchemaInferenceResult result = await InferAsync(
            $"[{{\"v\":{value}}}]");

        Assert.Equal(
            JsonTableColumnLogicalType.Text,
            Assert.Single(result.Columns).LogicalType);
    }

    [Fact]
    public async Task TypeSampleStopsButStructuralDiscoveryContinuesToEof()
    {
        JsonTableSchemaInferenceResult result = await InferAsync(
            """[{"a":1},{"a":2},{"late":"x"}]""",
            maxProfileRecords: 2);

        Assert.Equal(MigrationCoverageKind.Sample, result.TypeProfileCoverage.Kind);
        Assert.True(result.TypeProfileCoverage.RequiresFullStreamValidation);
        Assert.Equal(2, result.ProfileRecordsExamined);
        Assert.True(result.ProfileRecordLimitReached);
        Assert.Equal(["a", "late"], result.Columns.Select(item => item.OriginalPropertyName));
        Assert.Equal(JsonTableColumnLogicalType.SignedInteger, result.Columns[0].LogicalType);
        Assert.Equal(JsonTableInferenceConfidence.Medium, result.Columns[0].Confidence);
        Assert.Equal(JsonTableColumnLogicalType.Json, result.Columns[1].LogicalType);
        Assert.Equal(JsonTableInferenceConfidence.None, result.Columns[1].Confidence);
        Assert.Equal(2, result.Columns[1].MissingCount);
    }

    [Fact]
    public async Task SampledTypeLeavesAnIncompatibleUnseenTailForApplyValidation()
    {
        JsonTableSchemaInferenceResult result = await InferAsync(
            """[{"value":1},{"value":"late"}]""",
            maxProfileRecords: 1);

        JsonTableColumnSchema column = Assert.Single(result.Columns);
        Assert.Equal(
            JsonTableColumnLogicalType.SignedInteger,
            column.LogicalType);
        Assert.Equal(
            MigrationCoverageKind.Sample,
            result.TypeProfileCoverage.Kind);
        Assert.True(
            result.TypeProfileCoverage.RequiresFullStreamValidation);
        Assert.Equal(2, column.PresentCount);
        Assert.Equal(1, column.ProfiledNonNullCount);
        Assert.Contains(
            result.Diagnostics,
            item =>
                item.RuleId ==
                    JsonTableSchemaDiagnosticRules.SampledType &&
                item.ObjectId == JsonMigrationObjectIds.Column(0));
    }

    [Fact]
    public async Task OneSampledValueHasLowConfidence()
    {
        JsonTableSchemaInferenceResult result = await InferAsync(
            """[{"x":true},{"x":false}]""",
            maxProfileRecords: 1);

        JsonTableColumnSchema column = Assert.Single(result.Columns);
        Assert.Equal(JsonTableColumnLogicalType.Boolean, column.LogicalType);
        Assert.Equal(JsonTableInferenceConfidence.Low, column.Confidence);
        Assert.Contains(
            result.Diagnostics,
            item => item.RuleId == JsonTableSchemaDiagnosticRules.SampledType);
    }

    [Fact]
    public async Task ProfileByteLimitUsesWholeRecordCanonicalUtf8Size()
    {
        JsonTableSchemaInferenceResult exact = await InferAsync(
            """[{"x":"a"},{"x":"b"}]""",
            options: new JsonTableSchemaInferenceOptions { MaxProfileBytes = 6 });
        Assert.Equal(2, exact.ProfileRecordsExamined);
        Assert.Equal(6, exact.ProfileBytesExamined);
        Assert.Equal(MigrationCoverageKind.Full, exact.TypeProfileCoverage.Kind);

        JsonTableSchemaInferenceResult oneLess = await InferAsync(
            """[{"x":"a"},{"x":"b"}]""",
            options: new JsonTableSchemaInferenceOptions { MaxProfileBytes = 5 });
        Assert.Equal(1, oneLess.ProfileRecordsExamined);
        Assert.Equal(3, oneLess.ProfileBytesExamined);
        Assert.True(oneLess.ProfileByteLimitReached);
        Assert.Equal(MigrationCoverageKind.Sample, oneLess.TypeProfileCoverage.Kind);
    }

    [Fact]
    public async Task NestedCanonicalBytesControlProfileAdmission()
    {
        const string json = """[{"x":{"a":1}}]""";
        JsonTableSchemaInferenceResult exact = await InferAsync(
            json,
            options: new JsonTableSchemaInferenceOptions { MaxProfileBytes = 7 });
        Assert.Equal(7, exact.ProfileBytesExamined);
        Assert.Equal(MigrationCoverageKind.Full, exact.TypeProfileCoverage.Kind);

        JsonTableSchemaInferenceResult oneLess = await InferAsync(
            json,
            options: new JsonTableSchemaInferenceOptions { MaxProfileBytes = 6 });
        Assert.Equal(0, oneLess.ProfileRecordsExamined);
        Assert.True(oneLess.ProfileByteLimitReached);
        Assert.Equal(JsonTableColumnLogicalType.Json, Assert.Single(oneLess.Columns).LogicalType);
    }

    [Fact]
    public async Task DiscoverPerformsFullStructureScanWithoutTypeEvidence()
    {
        JsonTableSchemaInferenceResult result = await DiscoverAsync(
            """[{"a":1},{"b":"x"}]""");

        Assert.Equal(MigrationCoverageKind.Full, result.StructuralCoverage.Kind);
        Assert.Equal(MigrationCoverageKind.None, result.TypeProfileCoverage.Kind);
        Assert.Equal(0, result.ProfileRecordsExamined);
        Assert.Equal(["a", "b"], result.Columns.Select(item => item.OriginalPropertyName));
        Assert.All(
            result.Columns,
            item => Assert.Equal(JsonTableColumnLogicalType.Json, item.LogicalType));
    }

    [Fact]
    public async Task CompatibleOverrideIsOrdinalAndExactNameGuarded()
    {
        var schemaOverride = new JsonTableColumnSchemaOverride
        {
            ColumnIndex = 0,
            ExpectedPropertyName = "n",
            LogicalType = JsonTableColumnLogicalType.Json,
            Nullable = true,
        };
        JsonTableSchemaInferenceResult result = await InferAsync(
            """[{"n":1},{"n":"x"}]""",
            options: Options(schemaOverride));

        JsonTableColumnSchema column = Assert.Single(result.Columns);
        Assert.Equal(JsonTableColumnSchemaResolution.ExplicitOverride, column.Resolution);
        Assert.Equal(JsonTableInferenceConfidence.Explicit, column.Confidence);
        Assert.Equal(JsonTableOverrideValidationStatus.FullCompatible, column.OverrideValidation);
        Assert.True(column.Nullable);
    }

    [Fact]
    public async Task OverrideContradictionsRemainVisibleAndBlocking()
    {
        JsonTableSchemaInferenceResult kindMismatch = await InferAsync(
            """[{"x":"1"}]""",
            options: Options(
                new JsonTableColumnSchemaOverride
                {
                    ColumnIndex = 0,
                    ExpectedPropertyName = "x",
                    LogicalType = JsonTableColumnLogicalType.SignedInteger,
                }));
        JsonTableColumnSchema kindColumn = Assert.Single(kindMismatch.Columns);
        Assert.Equal(
            JsonTableOverrideValidationStatus.Incompatible,
            kindColumn.OverrideValidation);
        Assert.Contains(
            kindMismatch.Diagnostics,
            item => item.RuleId == JsonTableSchemaDiagnosticRules.OverrideMismatch);

        JsonTableSchemaInferenceResult nullMismatch = await InferAsync(
            """[{"x":null}]""",
            options: Options(
                new JsonTableColumnSchemaOverride
                {
                    ColumnIndex = 0,
                    ExpectedPropertyName = "x",
                    LogicalType = JsonTableColumnLogicalType.Json,
                    Nullable = false,
                }));
        Assert.Equal(
            JsonTableOverrideValidationStatus.Incompatible,
            Assert.Single(nullMismatch.Columns).OverrideValidation);
    }

    [Fact]
    public async Task InvalidOverrideRecipesFailDeterministically()
    {
        await Assert.ThrowsAsync<ArgumentException>(
            () => InferAsync(
                """[{"x":1}]""",
                options: Options(
                    new JsonTableColumnSchemaOverride
                    {
                        ColumnIndex = 0,
                        ExpectedPropertyName = "x",
                        LogicalType = JsonTableColumnLogicalType.SignedInteger,
                    },
                    new JsonTableColumnSchemaOverride
                    {
                        ColumnIndex = 0,
                        ExpectedPropertyName = "x",
                        LogicalType = JsonTableColumnLogicalType.Json,
                    })));

        await Assert.ThrowsAsync<ArgumentException>(
            () => InferAsync(
                """[{"x":1}]""",
                options: Options(
                    new JsonTableColumnSchemaOverride
                    {
                        ColumnIndex = 0,
                        ExpectedPropertyName = "y",
                        LogicalType = JsonTableColumnLogicalType.SignedInteger,
                    })));

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => InferAsync(
                """[{"x":1}]""",
                options: Options(
                    new JsonTableColumnSchemaOverride
                    {
                        ColumnIndex = 1,
                        ExpectedPropertyName = "y",
                        LogicalType = JsonTableColumnLogicalType.Json,
                    })));

        await Assert.ThrowsAsync<ArgumentException>(
            () => InferAsync(
                """[{"x":1}]""",
                options: Options(
                    new JsonTableColumnSchemaOverride
                    {
                        ColumnIndex = 0,
                        ExpectedPropertyName = "x",
                        LogicalType = JsonTableColumnLogicalType.Json,
                        Nullable = false,
                        MissingPolicy = JsonMissingPropertyPolicy.AsNull,
                    })));
    }

    [Fact]
    public async Task DistinctColumnLimitAcceptsExactAndRejectsOneMore()
    {
        JsonTableSchemaInferenceResult exact = await InferAsync(
            """[{"a":1,"b":2}]""",
            options: new JsonTableSchemaInferenceOptions { MaxColumns = 2 });
        Assert.Equal(2, exact.Columns.Count);

        JsonTableSchemaInferenceException exception =
            await Assert.ThrowsAsync<JsonTableSchemaInferenceException>(
                () => InferAsync(
                    """[{"a":1,"b":2,"c":3}]""",
                    options: new JsonTableSchemaInferenceOptions { MaxColumns = 2 }));
        Assert.Equal(JsonTableSchemaDiagnosticRules.ColumnLimit, exception.RuleId);
        Assert.Equal(2, exception.Limit);
        Assert.Equal(3, exception.Observed);
    }

    [Fact]
    public async Task AggregateNameByteLimitAcceptsExactAndRejectsOneMore()
    {
        JsonTableSchemaInferenceResult exact = await InferAsync(
            """[{"é":1}]""",
            options: new JsonTableSchemaInferenceOptions
            {
                MaxTotalColumnNameBytes = 2,
            });
        Assert.Equal(2, exact.TotalColumnNameBytes);

        JsonTableSchemaInferenceException exception =
            await Assert.ThrowsAsync<JsonTableSchemaInferenceException>(
                () => InferAsync(
                    """[{"é":1,"a":2}]""",
                    options: new JsonTableSchemaInferenceOptions
                    {
                        MaxTotalColumnNameBytes = 2,
                    }));
        Assert.Equal(
            JsonTableSchemaDiagnosticRules.ColumnNameBytesLimit,
            exception.RuleId);
        Assert.Equal(2, exception.Limit);
        Assert.Equal(3, exception.Observed);
    }

    [Fact]
    public async Task ReplayProducesTheSameSchemaAndDiagnosticIdentities()
    {
        const string json = """[{"a":1},{"b":"x"},{"a":-0}]""";
        JsonTableSchemaInferenceResult first = await InferAsync(
            json,
            maxProfileRecords: 2);
        JsonTableSchemaInferenceResult second = await InferAsync(
            json,
            maxProfileRecords: 2);

        Assert.Equal(first.Source.Fingerprint, second.Source.Fingerprint);
        Assert.Equal(
            first.Columns.Select(ColumnVector),
            second.Columns.Select(ColumnVector));
        Assert.Equal(
            first.Diagnostics.Select(item => item.DiagnosticId),
            second.Diagnostics.Select(item => item.DiagnosticId));
    }

    [Fact]
    public async Task InferenceRejectsSnapshotTamperingBeforePublishingSchema()
    {
        (JsonSourceSnapshot snapshot, JsonSourceBinding binding) =
            await BoundSourceAsync("""[{"value":1}]""");
        await using (snapshot)
        {
            string snapshotPath = Assert.IsType<string>(
                typeof(JsonSourceSnapshot)
                    .GetField(
                        "snapshotPath",
                        BindingFlags.Instance |
                        BindingFlags.NonPublic)!
                    .GetValue(snapshot));
            FileStream guard = Assert.IsType<FileStream>(
                typeof(JsonSourceSnapshot)
                    .GetField(
                        "integrityGuard",
                        BindingFlags.Instance |
                        BindingFlags.NonPublic)!
                    .GetValue(snapshot));
            await guard.DisposeAsync();
            await File.WriteAllBytesAsync(
                snapshotPath,
                Encoding.UTF8.GetBytes("""[{"value":2}]"""),
                TestContext.Current.CancellationToken);

            JsonSourceSnapshotException exception =
                await Assert.ThrowsAsync<JsonSourceSnapshotException>(
                    async () => await JsonTableSchemaInferer.InferAsync(
                        binding,
                        snapshot,
                        maxProfileRecords: 1,
                        cancellationToken:
                            TestContext.Current.CancellationToken));
            Assert.Equal(
                JsonSnapshotDiagnosticRules.IntegrityMismatch,
                exception.RuleId);
        }
    }

    [Fact]
    public async Task InferenceHonorsPreCanceledOperation()
    {
        (JsonSourceSnapshot snapshot, JsonSourceBinding binding) =
            await BoundSourceAsync("""[{"value":1}]""");
        await using (snapshot)
        using (var cancellation = new CancellationTokenSource())
        {
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await JsonTableSchemaInferer.InferAsync(
                    binding,
                    snapshot,
                    maxProfileRecords: 1,
                    cancellationToken: cancellation.Token));
        }
    }

    [Fact]
    public async Task MultipleValueFramingUsesTheSameTableContract()
    {
        JsonTableSchemaInferenceResult result = await InferAsync(
            "{\"a\":1}\n{\"b\":2}\nnull",
            framing: JsonInputFraming.MultipleValues);

        Assert.Equal(3, result.TotalRecords);
        Assert.Equal(2, result.EligibleObjectRecords);
        Assert.Equal(1, result.IneligibleRecords);
        Assert.Equal(["a", "b"], result.Columns.Select(item => item.OriginalPropertyName));
    }

    private static object ColumnVector(JsonTableColumnSchema column) => new
    {
        column.ColumnIndex,
        column.OriginalPropertyName,
        column.LogicalType,
        column.Resolution,
        column.Reason,
        column.Confidence,
        column.Nullable,
        column.PresentCount,
        column.NullCount,
        column.MissingCount,
    };

    private static JsonTableSchemaInferenceOptions Options(
        params JsonTableColumnSchemaOverride[] overrides) => new()
    {
        ColumnOverrides = overrides,
    };

    private static async Task<(
        JsonSourceSnapshot Snapshot,
        JsonSourceBinding Binding)> BoundSourceAsync(string json)
    {
        JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateAsync(
                new MemoryStream(Utf8Bytes(json)),
                cancellationToken:
                    TestContext.Current.CancellationToken);
        try
        {
            JsonSourceBinding binding =
                await JsonSourceBinding.CreateAsync(
                    snapshot,
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

    private static async Task<JsonTableSchemaInferenceResult> InferAsync(
        string json,
        int maxProfileRecords = 1_000,
        JsonTableSchemaInferenceOptions? options = null,
        JsonInputFraming framing = JsonInputFraming.RootArray)
    {
        using var workspace = new TemporaryWorkspace();
        await using JsonSourceSnapshot snapshot = await JsonSourceSnapshot.CreateAsync(
            new MemoryStream(Utf8Bytes(json)),
            workspace.Options(),
            TestContext.Current.CancellationToken);
        JsonSourceBinding binding = await JsonSourceBinding.CreateAsync(
            snapshot,
            new JsonStreamingReaderOptions { Framing = framing },
            cancellationToken: TestContext.Current.CancellationToken);
        return await JsonTableSchemaInferer.InferAsync(
            binding,
            snapshot,
            maxProfileRecords,
            options,
            TestContext.Current.CancellationToken);
    }

    private static async Task<JsonTableSchemaInferenceResult> DiscoverAsync(
        string json)
    {
        using var workspace = new TemporaryWorkspace();
        await using JsonSourceSnapshot snapshot = await JsonSourceSnapshot.CreateAsync(
            new MemoryStream(Utf8Bytes(json)),
            workspace.Options(),
            TestContext.Current.CancellationToken);
        JsonSourceBinding binding = await JsonSourceBinding.CreateAsync(
            snapshot,
            cancellationToken: TestContext.Current.CancellationToken);
        return await JsonTableSchemaInferer.DiscoverAsync(
            binding,
            snapshot,
            cancellationToken: TestContext.Current.CancellationToken);
    }

    private static byte[] Utf8Bytes(string value) =>
        new UTF8Encoding(false, true).GetBytes(value);

    private sealed class TemporaryWorkspace : IDisposable
    {
        internal TemporaryWorkspace()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-json-table-schema-tests-{Guid.NewGuid():N}");
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        internal JsonSourceSnapshotOptions Options() => new()
        {
            WorkspacePath = Root,
            MaxSourceBytes = 4 * 1024 * 1024,
        };

        public void Dispose()
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
    }
}
