using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonTypedMigrationDataSourceTests
{
    [Fact]
    public async Task AllTenCodecsApplyThroughTheMigrationRunnerWithoutLoss()
    {
        await using TypedSource origin = await TypedSource.CreateAsync(
            AllCodecJson,
            AllCodecOptions());
        MigrationCatalog catalog = Catalog(origin.Schema);
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 10);
        await using JsonMigrationDataSource source =
            await JsonMigrationDataSource.CreateAsync(
                origin.Schema,
                origin.Snapshot,
                catalog,
                Cancellation);
        await using var target =
            new JsonMigrationDataSourceIntegrationTests
                .ReceiptMigrationTarget();

        MigrationApplyResult result =
            await new MigrationApplyRunner().ApplyAsync(
                new MigrationApplyRequest
                {
                    Plan = plan,
                    Catalog = catalog,
                    Source = source,
                    Target = target,
                },
                Cancellation);

        Assert.Equal(1, result.RowsWritten);
        Assert.Equal(0, result.RowsSkipped);
        MigrationTargetBatch batch = Assert.Single(target.Batches);
        MigrationTargetRow row = Assert.Single(batch.Rows);
        Dictionary<string, DbValue> values = batch.ColumnObjectIds
            .Zip(row.Values)
            .ToDictionary(
                item => item.First,
                item => item.Second,
                StringComparer.Ordinal);
        Assert.Equal(DbType.Blob, values[JsonMigrationObjectIds.Column(0)].Type);
        Assert.Equal(DbType.Text, values[JsonMigrationObjectIds.Column(1)].Type);
        Assert.Equal(
            DbType.Integer,
            values[JsonMigrationObjectIds.Column(2)].Type);
        Assert.All(
            Enumerable.Range(3, 5),
            index => Assert.Equal(
                DbType.Text,
                values[JsonMigrationObjectIds.Column(index)].Type));
        Assert.Equal(
            DbType.Integer,
            values[JsonMigrationObjectIds.Column(8)].Type);
        Assert.Equal(DbType.Text, values[JsonMigrationObjectIds.Column(9)].Type);
        Assert.Equal(
            DbType.Text,
            values[JsonMigrationObjectIds.Column(10)].Type);
        Assert.Equal(
            [1, 2, 3, 4],
            values[JsonMigrationObjectIds.Column(0)].AsBlob.ToArray());
        Assert.Equal(
            "12345678901234567890.123456789012345678",
            values[JsonMigrationObjectIds.Column(1)].AsText);
        Assert.Equal(
            12_345,
            values[JsonMigrationObjectIds.Column(2)].AsInteger);
        Assert.Equal(
            "00112233-4455-6677-8899-aabbccddeeff",
            values[JsonMigrationObjectIds.Column(3)].AsText);
        Assert.Equal(
            "2024-02-29",
            values[JsonMigrationObjectIds.Column(4)].AsText);
        Assert.Equal(
            "08:09:10.1234567",
            values[JsonMigrationObjectIds.Column(5)].AsText);
        Assert.Equal(
            "2026-07-23 08:09:10.1234567",
            values[JsonMigrationObjectIds.Column(6)].AsText);
        Assert.Equal(
            "2026-07-23 08:09:10.1234567-07:00",
            values[JsonMigrationObjectIds.Column(7)].AsText);
        Assert.Equal(
            long.MinValue,
            values[JsonMigrationObjectIds.Column(8)].AsInteger);
        Assert.Equal(
            "18446744073709551615",
            values[JsonMigrationObjectIds.Column(9)].AsText);
        Assert.Equal(
            "ordinary",
            values[JsonMigrationObjectIds.Column(10)].AsText);
    }

    [Fact]
    public async Task TypedProfileMismatchesRemainDeterministicRowOutcomes()
    {
        const string json =
            """
            [
              {"value":"1"},
              {"value":2},
              {"value":null}
            ]
            """;
        await using TypedSource origin = await TypedSource.CreateAsync(
            json,
            OneCodecOptions(
                JsonTypedValueCodec.Int64String,
                nullable: false));
        MigrationCatalog catalog = Catalog(origin.Schema);
        Assert.Equal(
            JsonTableOverrideValidationStatus.Incompatible,
            Assert.Single(origin.Schema.Columns)
                .RepresentationSchema.OverrideValidation);
        Assert.DoesNotContain(
            origin.Schema.Diagnostics,
            diagnostic => diagnostic.RuleId ==
                JsonTableSchemaDiagnosticRules.OverrideMismatch);
        Assert.DoesNotContain(
            catalog.Diagnostics,
            diagnostic => diagnostic.RuleId ==
                JsonTableSchemaDiagnosticRules.OverrideMismatch);

        MigrationPlan plan = ReadyRejectPlan(
            catalog,
            batchSize: 10,
            JsonMigrationDataRules.TypedValueInvalid,
            JsonMigrationDataRules.NullNotAllowed);
        Assert.Equal(
            MigrationPlanReadinessStatus.Ready,
            MigrationPlanReadinessValidator
                .Evaluate(plan, catalog).Status);
        await using JsonMigrationDataSource source =
            await JsonMigrationDataSource.CreateAsync(
                origin.Schema,
                origin.Snapshot,
                catalog,
                Cancellation);
        await using var target =
            new JsonMigrationDataSourceIntegrationTests
                .ReceiptMigrationTarget();
        var request = new MigrationApplyRequest
        {
            Plan = plan,
            Catalog = catalog,
            Source = source,
            Target = target,
        };
        var runner = new MigrationApplyRunner();

        MigrationApplyResult applied =
            await runner.ApplyAsync(request, Cancellation);
        MigrationApplyResult replayed =
            await runner.ApplyAsync(request, Cancellation);

        Assert.Equal(1, applied.RowsWritten);
        Assert.Equal(2, applied.RejectedRowsWritten);
        Assert.Equal(0, replayed.RowsWritten);
        Assert.Equal(1, replayed.RowsSkipped);
        Assert.Equal(0, replayed.RejectedRowsWritten);
        Assert.Equal(2, replayed.RejectedRowsSkipped);
        Assert.Equal(
            [
                JsonMigrationDataRules.TypedValueInvalid,
                JsonMigrationDataRules.NullNotAllowed,
            ],
            target.Batches
                .SelectMany(batch => batch.RejectedRows)
                .OrderBy(row => row.SourceRowOrdinal)
                .Select(row => row.RuleId));
    }

    [Fact]
    public async Task AllTenCodecsEmitFrozenProviderNeutralSourceValues()
    {
        await using TypedSource origin = await TypedSource.CreateAsync(
            AllCodecJson,
            AllCodecOptions());
        MigrationCatalog catalog = Catalog(origin.Schema);
        await using JsonMigrationDataSource source =
            await JsonMigrationDataSource.CreateAsync(
                origin.Schema,
                origin.Snapshot,
                catalog,
                Cancellation);

        Assert.Equal(
            "csharpdb-json-cursor/v2",
            JsonMigrationDataSource.TypedCursorAlgorithmId);
        Assert.Contains(
            JsonMigrationDataRules.TypedValueInvalid,
            source.SupportedRejectRuleIds);

        MigrationDataBatch batch = Assert.Single(
            await CollectAsync(
                source.ReadAsync(
                    Request(
                        source,
                        AllColumns(origin.Schema),
                        batchSize: 10),
                    Cancellation)));
        MigrationDataRow row = Assert.Single(batch.Rows);
        Assert.Empty(batch.RejectedRows);
        Assert.Equal(
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
                MigrationSourceValueKind.Text,
            ],
            row.Values.Select(item => item.Kind));

        Assert.Null(row.Values[0].CanonicalText);
        Assert.Equal(
            [1, 2, 3, 4],
            row.Values[0].BinaryValue.ToArray());
        Assert.Equal(
            [
                "12345678901234567890.123456789012345678",
                "123.45",
                "00112233-4455-6677-8899-aabbccddeeff",
                "2024-02-29",
                "08:09:10.1234567",
                "2026-07-23 08:09:10.1234567",
                "2026-07-23 08:09:10.1234567-07:00",
                "-9223372036854775808",
                "18446744073709551615",
                "ordinary",
            ],
            row.Values.Skip(1).Select(item => item.CanonicalText));
        Assert.All(
            row.Values.Skip(1),
            item => Assert.True(item.BinaryValue.IsEmpty));
    }

    [Fact]
    public async Task HighPrecisionScaleEqualPrecisionMapsAndConvertsExactly()
    {
        const string decimalText =
            "0.12345678901234567890123456789012345678";
        await using TypedSource origin = await TypedSource.CreateAsync(
            $"[{{\"value\":\"{decimalText}\"}}]",
            OneCodecOptions(
                JsonTypedValueCodec.DecimalString,
                precision: 38,
                scale: 38));
        MigrationCatalog catalog = Catalog(origin.Schema);
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        MigrationCatalogObject column = Assert.Single(
            catalog.Objects,
            item => item.ObjectId ==
                    JsonMigrationObjectIds.Column(0));
        MigrationTypeMapping mapping = Assert.Single(
            Assert.Single(
                plan.Objects,
                item => item.SourceObjectId ==
                        JsonMigrationObjectIds.Column(0))
                .TypeMappings);
        Assert.Equal(DbType.Text, mapping.TargetType);
        Assert.Equal(
            "json-typed-decimal-text",
            mapping.Conversion?.ConversionId);

        await using JsonMigrationDataSource source =
            await JsonMigrationDataSource.CreateAsync(
                origin.Schema,
                origin.Snapshot,
                catalog,
                Cancellation);
        await using var target =
            new JsonMigrationDataSourceIntegrationTests
                .ReceiptMigrationTarget();
        var request = new MigrationApplyRequest
        {
            Plan = ReadyPlan(catalog, batchSize: 10),
            Catalog = catalog,
            Source = source,
            Target = target,
        };
        var runner = new MigrationApplyRunner();

        MigrationApplyResult applied =
            await runner.ApplyAsync(request, Cancellation);
        MigrationApplyResult replayed =
            await runner.ApplyAsync(request, Cancellation);

        Assert.Equal(1, applied.RowsWritten);
        Assert.Equal(0, replayed.RowsWritten);
        Assert.Equal(1, replayed.RowsSkipped);
        DbValue converted = Assert.Single(
            Assert.Single(
                Assert.Single(target.Batches).Rows).Values);
        Assert.Equal(DbType.Text, converted.Type);
        Assert.Equal(decimalText, converted.AsText);
    }

    public static TheoryData<
        JsonTypedValueCodec,
        string,
        int,
        int> InvalidCanonicalValues => new()
        {
            {
                JsonTypedValueCodec.BinaryBase64,
                "\"AQI\"",
                38,
                18
            },
            {
                JsonTypedValueCodec.DecimalString,
                "\"01.2\"",
                38,
                18
            },
            {
                JsonTypedValueCodec.DecimalNumber,
                "1e2",
                38,
                18
            },
            {
                JsonTypedValueCodec.GuidD,
                "\"00112233-4455-6677-8899-AABBCCDDEEFF\"",
                38,
                18
            },
            {
                JsonTypedValueCodec.DateCSharpDbText,
                "\"2024-2-29\"",
                38,
                18
            },
            {
                JsonTypedValueCodec.TimeCSharpDbText,
                "\"08:09:10.1\"",
                38,
                18
            },
            {
                JsonTypedValueCodec.DateTimeCSharpDbText,
                "\"2026-07-23T08:09:10\"",
                38,
                18
            },
            {
                JsonTypedValueCodec.DateTimeOffsetCSharpDbText,
                "\"2026-07-23 08:09:10Z\"",
                38,
                18
            },
            {
                JsonTypedValueCodec.Int64String,
                "\"-0\"",
                38,
                18
            },
            {
                JsonTypedValueCodec.UInt64String,
                "\"+1\"",
                38,
                18
            },
            {
                JsonTypedValueCodec.DecimalString,
                "\"1234\"",
                3,
                0
            },
            {
                JsonTypedValueCodec.DecimalNumber,
                "1.0",
                3,
                2
            },
        };

    [Theory]
    [MemberData(nameof(InvalidCanonicalValues))]
    public async Task NonCanonicalOrFacetInvalidValuesUseOneTypedRowRule(
        JsonTypedValueCodec codec,
        string jsonValue,
        int precision,
        int scale)
    {
        string json = $"[{{\"value\":{jsonValue}}}]";
        await using TypedSource origin = await TypedSource.CreateAsync(
            json,
            OneCodecOptions(
                codec,
                precision: precision,
                scale: scale),
            discover: true);
        await using JsonMigrationDataSource source =
            await CreateDataSourceAsync(origin);
        string[] columns = AllColumns(origin.Schema);
        MigrationReadRequest request =
            Request(source, columns, batchSize: 10);

        MigrationRowRejectedException failFast =
            await Assert.ThrowsAsync<MigrationRowRejectedException>(
                async () => await CollectAsync(
                    source.ReadAsync(request, Cancellation)));
        Assert.Equal(
            JsonMigrationDataRules.TypedValueInvalid,
            failFast.Code);

        MigrationDataBatch rejected = Assert.Single(
            await CollectAsync(
                source.ReadAsync(
                    RejectRequest(
                        source,
                        columns,
                        batchSize: 10),
                    Cancellation)));
        Assert.Empty(rejected.Rows);
        MigrationRejectedRow row =
            Assert.Single(rejected.RejectedRows);
        Assert.Equal(
            JsonMigrationDataRules.TypedValueInvalid,
            row.RuleId);
        Assert.Equal(
            JsonMigrationObjectIds.Column(0),
            row.ColumnObjectId);
        Assert.Contains(
            row.Evidence,
            item => item.Name ==
                    MigrationRejectLedgerCodec
                        .RawValueEvidenceName &&
                    item.Value == jsonValue);
    }

    [Theory]
    [InlineData(
        JsonTypedValueCodec.BinaryBase64,
        "1")]
    [InlineData(
        JsonTypedValueCodec.DecimalNumber,
        "\"1\"")]
    [InlineData(
        JsonTypedValueCodec.Int64String,
        "1")]
    public async Task WrongJsonKindIsRowLocalBeforeLexicalValidation(
        JsonTypedValueCodec codec,
        string jsonValue)
    {
        await using TypedSource origin = await TypedSource.CreateAsync(
            $"[{{\"value\":{jsonValue}}}]",
            OneCodecOptions(codec),
            discover: true);
        await using JsonMigrationDataSource source =
            await CreateDataSourceAsync(origin);

        MigrationDataBatch batch = Assert.Single(
            await CollectAsync(
                source.ReadAsync(
                    RejectRequest(
                        source,
                        AllColumns(origin.Schema),
                        batchSize: 10),
                    Cancellation)));
        Assert.Equal(
            JsonMigrationDataRules.TypedValueInvalid,
            Assert.Single(batch.RejectedRows).RuleId);
    }

    [Fact]
    public async Task NullAndMissingPoliciesRemainDistinctDuringReplay()
    {
        const string json =
            """[{"value":null},{},{"value":"AA=="}]""";
        await using TypedSource strict = await TypedSource.CreateAsync(
            json,
            OneCodecOptions(
                JsonTypedValueCodec.BinaryBase64,
                nullable: true),
            discover: true);
        await using JsonMigrationDataSource strictSource =
            await CreateDataSourceAsync(strict);
        MigrationDataBatch strictBatch = Assert.Single(
            await CollectAsync(
                strictSource.ReadAsync(
                    RejectRequest(
                        strictSource,
                        AllColumns(strict.Schema),
                        batchSize: 10),
                    Cancellation)));

        Assert.Equal(2, strictBatch.Rows.Count);
        Assert.Equal(
            MigrationSourceValueKind.Null,
            strictBatch.Rows[0].Values[0].Kind);
        Assert.Equal(
            [0],
            strictBatch.Rows[1].Values[0].BinaryValue.ToArray());
        Assert.Equal(
            JsonMigrationDataRules.MissingProperty,
            Assert.Single(strictBatch.RejectedRows).RuleId);

        await using TypedSource asNull = await TypedSource.CreateAsync(
            json,
            OneCodecOptions(
                JsonTypedValueCodec.BinaryBase64,
                nullable: true,
                missingPolicy:
                    JsonMissingPropertyPolicy.AsNull),
            discover: true);
        await using JsonMigrationDataSource asNullSource =
            await CreateDataSourceAsync(asNull);
        MigrationDataBatch asNullBatch = Assert.Single(
            await CollectAsync(
                asNullSource.ReadAsync(
                    Request(
                        asNullSource,
                        AllColumns(asNull.Schema),
                        batchSize: 10),
                    Cancellation)));
        Assert.Empty(asNullBatch.RejectedRows);
        Assert.Equal(
            [
                MigrationSourceValueKind.Null,
                MigrationSourceValueKind.Null,
                MigrationSourceValueKind.Binary,
            ],
            asNullBatch.Rows.Select(
                item => item.Values[0].Kind));
    }

    [Fact]
    public async Task ExplicitNonNullableNullUsesExistingNullRule()
    {
        await using TypedSource origin = await TypedSource.CreateAsync(
            """[{"value":null}]""",
            OneCodecOptions(
                JsonTypedValueCodec.Int64String,
                nullable: false),
            discover: true);
        await using JsonMigrationDataSource source =
            await CreateDataSourceAsync(origin);

        MigrationDataBatch batch = Assert.Single(
            await CollectAsync(
                source.ReadAsync(
                    RejectRequest(
                        source,
                        AllColumns(origin.Schema),
                        batchSize: 10),
                    Cancellation)));
        Assert.Equal(
            JsonMigrationDataRules.NullNotAllowed,
            Assert.Single(batch.RejectedRows).RuleId);
    }

    [Fact]
    public async Task RetainedBinaryCeilingIsFatalEvenWhenRejectsAreEnabled()
    {
        await using TypedSource origin = await TypedSource.CreateAsync(
            """[{"value":"AQI="}]""",
            OneCodecOptions(
                JsonTypedValueCodec.BinaryBase64,
                maxDecodedBinaryBytes: 1),
            discover: true);
        await using JsonMigrationDataSource source =
            await CreateDataSourceAsync(origin);

        MigrationRowRejectedException error =
            await Assert.ThrowsAsync<MigrationRowRejectedException>(
                async () => await CollectAsync(
                    source.ReadAsync(
                        RejectRequest(
                            source,
                            AllColumns(origin.Schema),
                            batchSize: 10),
                        Cancellation)));
        Assert.Equal(
            JsonMigrationDataRules.ValueSizeExceeded,
            error.Code);
    }

    [Fact]
    public async Task RetainedDecimalDigitCeilingWinsOverDeclaredFacetMismatch()
    {
        await using TypedSource origin = await TypedSource.CreateAsync(
            """[{"value":"1234"}]""",
            OneCodecOptions(
                JsonTypedValueCodec.DecimalString,
                precision: 3,
                scale: 0,
                maxDecimalDigits: 3),
            discover: true);
        await using JsonMigrationDataSource source =
            await CreateDataSourceAsync(origin);

        MigrationRowRejectedException error =
            await Assert.ThrowsAsync<MigrationRowRejectedException>(
                async () => await CollectAsync(
                    source.ReadAsync(
                        RejectRequest(
                            source,
                            AllColumns(origin.Schema),
                            batchSize: 10),
                        Cancellation)));
        Assert.Equal(
            JsonMigrationDataRules.ValueSizeExceeded,
            error.Code);
    }

    [Fact]
    public async Task RetainedDecimalLimitPreflightWinsForMalformedOversizeText()
    {
        await using TypedSource origin = await TypedSource.CreateAsync(
            """[{"value":"1234x0"}]""",
            OneCodecOptions(
                JsonTypedValueCodec.DecimalString,
                precision: 3,
                scale: 0,
                maxDecimalDigits: 3),
            discover: true);
        await using JsonMigrationDataSource source =
            await CreateDataSourceAsync(origin);

        MigrationRowRejectedException error =
            await Assert.ThrowsAsync<MigrationRowRejectedException>(
                async () => await CollectAsync(
                    source.ReadAsync(
                        RejectRequest(
                            source,
                            AllColumns(origin.Schema),
                            batchSize: 10),
                        Cancellation)));
        Assert.Equal(
            JsonMigrationDataRules.ValueSizeExceeded,
            error.Code);
    }

    [Fact]
    public async Task RequestValueCeilingRemainsFatalForDecodedBinary()
    {
        await using TypedSource origin = await TypedSource.CreateAsync(
            """[{"value":"AQIDBA=="}]""",
            OneCodecOptions(
                JsonTypedValueCodec.BinaryBase64),
            discover: true);
        await using JsonMigrationDataSource source =
            await CreateDataSourceAsync(origin);

        MigrationRowRejectedException error =
            await Assert.ThrowsAsync<MigrationRowRejectedException>(
                async () => await CollectAsync(
                    source.ReadAsync(
                        RejectRequest(
                            source,
                            AllColumns(origin.Schema),
                            batchSize: 10,
                            maxValueBytes: 8),
                        Cancellation)));
        Assert.Equal(
            JsonMigrationDataRules.ValueSizeExceeded,
            error.Code);
    }

    [Fact]
    public async Task TypedCursorV2ReplaysEverySuffixExactly()
    {
        const string json =
            """
            [
              {"value":"1"},
              {"value":"2"},
              {"value":"bad"},
              {"value":"4"},
              {"value":"5"}
            ]
            """;
        await using TypedSource origin = await TypedSource.CreateAsync(
            json,
            OneCodecOptions(
                JsonTypedValueCodec.Int64String),
            discover: true);
        await using JsonMigrationDataSource source =
            await CreateDataSourceAsync(origin);
        MigrationReadRequest request = RejectRequest(
            source,
            AllColumns(origin.Schema),
            batchSize: 2);
        List<MigrationDataBatch> batches = await CollectAsync(
            source.ReadAsync(request, Cancellation));

        Assert.Equal([2, 2, 1], batches.Select(OutcomeCount));
        Assert.StartsWith(
            JsonMigrationDataSource.TypedCursorAlgorithmId + "/",
            batches[0].NextCursor,
            StringComparison.Ordinal);
        Assert.Null(batches[^1].NextCursor);

        for (int boundary = 0;
             boundary < batches.Count - 1;
             boundary++)
        {
            string cursor = Assert.IsType<string>(
                batches[boundary].NextCursor);
            List<MigrationDataBatch> resumed =
                await CollectAsync(
                    source.ReadAsync(
                        request with
                        {
                            ResumeCursor = cursor,
                        },
                        Cancellation));
            Assert.Equal(
                batches.Count - boundary - 1,
                resumed.Count);
            for (int index = 0;
                 index < resumed.Count;
                 index++)
            {
                AssertBatchEqual(
                    batches[boundary + index + 1],
                    resumed[index]);
            }
        }
    }

    [Fact]
    public async Task IntentDigestChangeInvalidatesTypedCursor()
    {
        const string json =
            """
            [
              {"value":"1"},
              {"value":"2"},
              {"value":"3"}
            ]
            """;
        await using TypedSource required = await TypedSource.CreateAsync(
            json,
            OneCodecOptions(
                JsonTypedValueCodec.Int64String,
                nullable: false),
            discover: true);
        await using TypedSource nullable = await TypedSource.CreateAsync(
            json,
            OneCodecOptions(
                JsonTypedValueCodec.Int64String,
                nullable: true),
            discover: true);
        await using JsonMigrationDataSource first =
            await CreateDataSourceAsync(required);
        await using JsonMigrationDataSource second =
            await CreateDataSourceAsync(nullable);
        MigrationReadRequest firstRequest = Request(
            first,
            AllColumns(required.Schema),
            batchSize: 1);
        string cursor = Assert.IsType<string>(
            (await CollectAsync(
                first.ReadAsync(firstRequest, Cancellation)))[0]
            .NextCursor);

        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await CollectAsync(
                second.ReadAsync(
                    Request(
                        second,
                        AllColumns(nullable.Schema),
                        batchSize: 1) with
                    {
                        ResumeCursor = cursor,
                    },
                    Cancellation)));
    }

    [Fact]
    public async Task TypedV2AndOrdinaryV1CursorsAreMutuallyInvalid()
    {
        const string json =
            """
            [
              {"value":"1"},
              {"value":"2"},
              {"value":"3"}
            ]
            """;
        await using TypedSource origin = await TypedSource.CreateAsync(
            json,
            OneCodecOptions(
                JsonTypedValueCodec.Int64String),
            discover: true);
        JsonTableSchemaInferenceResult ordinarySchema =
            await JsonTableSchemaInferer.DiscoverAsync(
                origin.Binding,
                origin.Snapshot,
                cancellationToken: Cancellation);
        await using JsonMigrationDataSource typed =
            await CreateDataSourceAsync(origin);
        await using JsonMigrationDataSource ordinary =
            await JsonMigrationDataSource.CreateAsync(
                ordinarySchema,
                origin.Snapshot,
                ordinarySchema.CreateCatalog(
                    CSharpDbCapabilityCatalogLoader
                        .CurrentTargetVersion),
                Cancellation);
        MigrationReadRequest typedRequest = Request(
            typed,
            AllColumns(origin.Schema),
            batchSize: 1);
        MigrationReadRequest ordinaryRequest = Request(
            ordinary,
            [JsonMigrationObjectIds.Column(0)],
            batchSize: 1);
        string typedCursor = Assert.IsType<string>(
            (await CollectAsync(
                typed.ReadAsync(
                    typedRequest,
                    Cancellation)))[0].NextCursor);
        string ordinaryCursor = Assert.IsType<string>(
            (await CollectAsync(
                ordinary.ReadAsync(
                    ordinaryRequest,
                    Cancellation)))[0].NextCursor);

        Assert.StartsWith(
            JsonMigrationDataSource.TypedCursorAlgorithmId + "/",
            typedCursor,
            StringComparison.Ordinal);
        Assert.StartsWith(
            JsonMigrationDataSource.CursorAlgorithmId + "/",
            ordinaryCursor,
            StringComparison.Ordinal);
        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await CollectAsync(
                typed.ReadAsync(
                    typedRequest with
                    {
                        ResumeCursor = ordinaryCursor,
                    },
                    Cancellation)));
        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await CollectAsync(
                ordinary.ReadAsync(
                    ordinaryRequest with
                    {
                        ResumeCursor = typedCursor,
                    },
                    Cancellation)));
    }

    [Fact]
    public async Task TypedCatalogTamperIsRejectedBeforeSourceCreation()
    {
        await using TypedSource origin = await TypedSource.CreateAsync(
            """[{"value":"1"}]""",
            OneCodecOptions(
                JsonTypedValueCodec.Int64String));
        MigrationCatalog catalog = Catalog(origin.Schema);
        MigrationCatalog tampered = catalog with
        {
            Objects = catalog.Objects.Select(item =>
                item.ObjectId == JsonMigrationObjectIds.Table
                    ? item with
                    {
                        Facets = item.Facets.Select(facet =>
                            facet.Name ==
                            "jsonTypedIntentManifestDigest"
                                ? facet with
                                {
                                    Value =
                                        "sha256:" +
                                        new string('0', 64),
                                }
                                : facet).ToArray(),
                    }
                    : item).ToArray(),
        };

        await Assert.ThrowsAsync<ArgumentException>(
            async () => await JsonMigrationDataSource.CreateAsync(
                origin.Schema,
                origin.Snapshot,
                tampered,
                Cancellation));
    }

    private static async Task<JsonMigrationDataSource>
        CreateDataSourceAsync(TypedSource origin) =>
        await JsonMigrationDataSource.CreateAsync(
            origin.Schema,
            origin.Snapshot,
            Catalog(origin.Schema),
            Cancellation);

    private static MigrationCatalog Catalog(
        JsonTypedTableSchemaInferenceResult schema) =>
        schema.CreateCatalog(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);

    private static MigrationPlan ReadyPlan(
        MigrationCatalog catalog,
        int batchSize)
    {
        MigrationPlan draft =
            new MigrationPlanner().CreatePlan(catalog);
        return draft with
        {
            AcceptedExclusionObjectIds = draft.Objects
                .Where(item => !item.Included)
                .Select(item => item.SourceObjectId)
                .OrderBy(item => item, StringComparer.Ordinal)
                .ToArray(),
            Load = draft.Load with
            {
                BatchSize = batchSize,
            },
        };
    }

    private static MigrationPlan ReadyRejectPlan(
        MigrationCatalog catalog,
        int batchSize,
        params string[] allowedRuleIds)
    {
        MigrationPlan failFast =
            ReadyPlan(catalog, batchSize);
        return failFast with
        {
            Load = failFast.Load with
            {
                RejectMode =
                    MigrationRejectMode.DeterministicRejects,
                RejectPolicy =
                    new MigrationDeterministicRejectPolicy
                    {
                        ContractVersion =
                            MigrationRejectContract
                                .DeterministicRejectsV1,
                        AllowedRuleIds = allowedRuleIds,
                        MaxRejectedRowsPerBatch = batchSize,
                        MaxRejectedRowsPerRun = 1_000,
                        MaxRawValueBytes = 4_096,
                        MaxRawValueBytesPerBatch =
                            64 * 1_024,
                        MaxRawValueBytesPerRun =
                            1024 * 1_024,
                        MaxArtifactBytes =
                            16 * 1_024 * 1_024,
                    },
            },
        };
    }

    private static string[] AllColumns(
        JsonTypedTableSchemaInferenceResult schema) =>
        Enumerable.Range(0, schema.Columns.Count)
            .Select(JsonMigrationObjectIds.Column)
            .ToArray();

    private static MigrationReadRequest Request(
        JsonMigrationDataSource source,
        IReadOnlyList<string> columns,
        int batchSize,
        long maxBatchBytes = 64L * 1024 * 1024,
        int maxValueBytes = 16 * 1024 * 1024) => new()
        {
            SourceObjectId = JsonMigrationObjectIds.Table,
            ColumnObjectIds = columns,
            BatchSize = batchSize,
            MaxBatchBytes = maxBatchBytes,
            MaxValueBytes = maxValueBytes,
            SnapshotToken = source.SnapshotIdentity,
        };

    private static MigrationReadRequest RejectRequest(
        JsonMigrationDataSource source,
        IReadOnlyList<string> columns,
        int batchSize,
        long maxBatchBytes = 64L * 1024 * 1024,
        int maxValueBytes = 16 * 1024 * 1024) =>
        Request(
            source,
            columns,
            batchSize,
            maxBatchBytes,
            maxValueBytes) with
        {
            RejectContractVersion =
                MigrationRejectContract.DeterministicRejectsV1,
            RejectPolicy = new MigrationDeterministicRejectPolicy
            {
                ContractVersion =
                    MigrationRejectContract.DeterministicRejectsV1,
                AllowedRuleIds =
                [
                    JsonMigrationDataRules.NonObjectRow,
                    JsonMigrationDataRules.MissingProperty,
                    JsonMigrationDataRules.NullNotAllowed,
                    JsonMigrationDataRules.TypeMismatch,
                    JsonMigrationDataRules.TypedValueInvalid,
                ],
                MaxRejectedRowsPerBatch = batchSize,
                MaxRejectedRowsPerRun = 1_000,
                MaxRawValueBytes = 4_096,
                MaxRawValueBytesPerBatch = 64 * 1_024,
                MaxRawValueBytesPerRun = 1024 * 1_024,
                MaxArtifactBytes = 16 * 1_024 * 1_024,
            },
        };

    private static int OutcomeCount(
        MigrationDataBatch batch) =>
        checked(
            batch.Rows.Count +
            batch.RejectedRows.Count);

    private static void AssertBatchEqual(
        MigrationDataBatch expected,
        MigrationDataBatch actual)
    {
        Assert.Equal(expected.SourceObjectId, actual.SourceObjectId);
        Assert.Equal(expected.SnapshotIdentity, actual.SnapshotIdentity);
        Assert.Equal(expected.ColumnObjectIds, actual.ColumnObjectIds);
        Assert.Equal(expected.BatchOrdinal, actual.BatchOrdinal);
        Assert.Equal(expected.StartCursor, actual.StartCursor);
        Assert.Equal(expected.NextCursor, actual.NextCursor);
        Assert.Equal(expected.Rows.Count, actual.Rows.Count);
        for (int rowIndex = 0;
             rowIndex < expected.Rows.Count;
             rowIndex++)
        {
            MigrationDataRow left = expected.Rows[rowIndex];
            MigrationDataRow right = actual.Rows[rowIndex];
            Assert.Equal(left.StableKey, right.StableKey);
            Assert.Equal(left.Values.Count, right.Values.Count);
            for (int valueIndex = 0;
                 valueIndex < left.Values.Count;
                 valueIndex++)
            {
                Assert.Equal(
                    left.Values[valueIndex].Kind,
                    right.Values[valueIndex].Kind);
                Assert.Equal(
                    left.Values[valueIndex].CanonicalText,
                    right.Values[valueIndex].CanonicalText);
                Assert.Equal(
                    left.Values[valueIndex].BinaryValue.ToArray(),
                    right.Values[valueIndex].BinaryValue.ToArray());
            }
        }

        Assert.Equal(
            expected.RejectedRows.Select(item =>
                (
                    item.SourceRowOrdinal,
                    item.RuleId,
                    item.ColumnObjectId,
                    Evidence: item.Evidence.Select(evidence =>
                        (evidence.Name, evidence.Value)).ToArray())),
            actual.RejectedRows.Select(item =>
                (
                    item.SourceRowOrdinal,
                    item.RuleId,
                    item.ColumnObjectId,
                    Evidence: item.Evidence.Select(evidence =>
                        (evidence.Name, evidence.Value)).ToArray())));
    }

    private static async Task<List<T>> CollectAsync<T>(
        IAsyncEnumerable<T> values)
    {
        var result = new List<T>();
        await foreach (T value in values.WithCancellation(
                           Cancellation))
        {
            result.Add(value);
        }

        return result;
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
            "ordinary":"ordinary"
          }
        ]
        """;

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    private static byte[] Utf8Bytes(string value) =>
        new UTF8Encoding(false, true).GetBytes(value);

    private sealed class TypedSource : IAsyncDisposable
    {
        private readonly TemporaryDirectory directory;

        private TypedSource(
            TemporaryDirectory directory,
            JsonSourceSnapshot snapshot,
            JsonSourceBinding binding,
            JsonTypedIntentManifest intent,
            JsonTypedTableSchemaInferenceResult schema)
        {
            this.directory = directory;
            Snapshot = snapshot;
            Binding = binding;
            Intent = intent;
            Schema = schema;
        }

        internal JsonSourceSnapshot Snapshot { get; }

        internal JsonSourceBinding Binding { get; }

        internal JsonTypedIntentManifest Intent { get; }

        internal JsonTypedTableSchemaInferenceResult Schema { get; }

        internal static async Task<TypedSource> CreateAsync(
            string json,
            JsonTypedIntentOptions intentOptions,
            bool discover = false)
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
                string sidecarPath = Path.Combine(
                    directory.Root,
                    "intent" +
                    JsonTypedIntentSidecar.FileExtension);
                JsonTypedIntentManifest intent =
                    await JsonTypedIntentSidecar.WriteAsync(
                        sidecarPath,
                        binding,
                        intentOptions,
                        Cancellation);
                JsonTypedTableSchemaInferenceResult schema =
                    discover
                        ? await JsonTypedTableSchemaInferer
                            .DiscoverAsync(
                                binding,
                                snapshot,
                                intent,
                                cancellationToken:
                                    Cancellation)
                        : await JsonTypedTableSchemaInferer
                            .InferAsync(
                                binding,
                                snapshot,
                                intent,
                                maxProfileRecords: 100,
                                cancellationToken:
                                    Cancellation);
                return new TypedSource(
                    directory,
                    snapshot,
                    binding,
                    intent,
                    schema);
            }
            catch
            {
                if (snapshot is not null)
                    await snapshot.DisposeAsync();
                directory.Dispose();
                throw;
            }
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
                "csharpdb-json-typed-source-tests-" +
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
