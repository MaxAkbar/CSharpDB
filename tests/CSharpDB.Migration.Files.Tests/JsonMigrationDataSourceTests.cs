using System.Reflection;
using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonMigrationDataSourceTests
{
    [Fact]
    public async Task CreationRejectsDifferentSnapshotsAndCatalogPolicyDrift()
    {
        (JsonSourceSnapshot expectedSnapshot, JsonTableSchemaInferenceResult schema) =
            await InferAsync("""[{"id":1},{"id":2}]""");
        (JsonSourceSnapshot differentSnapshot, _) =
            await InferAsync("""[{"id":3},{"id":4}]""");
        await using (expectedSnapshot)
        await using (differentSnapshot)
        {
            ArgumentException snapshotError =
                await Assert.ThrowsAsync<ArgumentException>(
                    async () => await JsonMigrationDataSource.CreateAsync(
                        schema,
                        differentSnapshot,
                        Catalog(schema),
                        Cancellation));
            Assert.Equal("snapshot", snapshotError.ParamName);

            MigrationCatalog catalog = Catalog(schema);
            MigrationCatalog drifted = catalog with
            {
                Objects = catalog.Objects
                    .Select(item =>
                        item.ObjectId == JsonMigrationObjectIds.Table
                            ? item with { SourceName = "different_table" }
                            : item)
                    .ToArray(),
            };
            await Assert.ThrowsAsync<ArgumentException>(
                async () => await JsonMigrationDataSource.CreateAsync(
                    schema,
                    expectedSnapshot,
                    drifted,
                    Cancellation));
        }
    }

    [Fact]
    public async Task InvalidColumnProjectionsAreRejectedSynchronously()
    {
        (JsonSourceSnapshot snapshot, JsonTableSchemaInferenceResult schema) =
            await InferAsync("""[{"left":"a","right":"b"}]""");
        await using (snapshot)
        await using (JsonMigrationDataSource source =
                     await JsonMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            MigrationReadRequest request = Request(
                source,
                [JsonMigrationObjectIds.Column(0), JsonMigrationObjectIds.Column(1)],
                batchSize: 1);

            Assert.Throws<ArgumentException>(() => source.ReadAsync(
                request with { SourceObjectId = "json:table:1" },
                Cancellation));
            Assert.Throws<ArgumentException>(() => source.ReadAsync(
                request with { ColumnObjectIds = [] },
                Cancellation));
            Assert.Throws<ArgumentException>(() => source.ReadAsync(
                request with
                {
                    ColumnObjectIds =
                    [
                        JsonMigrationObjectIds.Column(0),
                        JsonMigrationObjectIds.Column(0),
                    ],
                },
                Cancellation));
            Assert.Throws<ArgumentException>(() => source.ReadAsync(
                request with { ColumnObjectIds = ["json:column:01"] },
                Cancellation));
            Assert.Throws<ArgumentException>(() => source.ReadAsync(
                request with
                {
                    ColumnObjectIds =
                    [
                        JsonMigrationObjectIds.Column(99),
                    ],
                },
                Cancellation));
        }
    }

    [Fact]
    public async Task EveryLogicalTypeAndCanonicalJsonEmitFrozenSourceRepresentations()
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
                "payload":{"b":2,"a":["x",1e2,-0]}
              },
              {
                "text":"beta",
                "flag":false,
                "signed":2,
                "unsigned":9223372036854775808,
                "amount":2.5,
                "payload":"line\n"
              }
            ]
            """;
        (JsonSourceSnapshot snapshot, JsonTableSchemaInferenceResult schema) =
            await InferAsync(json);
        await using (snapshot)
        await using (JsonMigrationDataSource source =
                     await JsonMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            string[] columns = Enumerable.Range(0, schema.Columns.Count)
                .Select(JsonMigrationObjectIds.Column)
                .ToArray();
            MigrationDataBatch batch = Assert.Single(await CollectAsync(
                source.ReadAsync(
                    Request(source, columns, batchSize: 10),
                    Cancellation)));
            Assert.Equal(2, batch.Rows.Count);

            MigrationDataRow first = batch.Rows[0];
            Assert.Equal(
                [
                    MigrationSourceValueKind.Text,
                    MigrationSourceValueKind.Boolean,
                    MigrationSourceValueKind.SignedInteger,
                    MigrationSourceValueKind.UnsignedInteger,
                    MigrationSourceValueKind.Decimal,
                    MigrationSourceValueKind.Text,
                ],
                first.Values.Select(value => value.Kind));
            Assert.Equal(
                [
                    "alpha",
                    "true",
                    "-1",
                    "18446744073709551615",
                    "1.25",
                    """{"b":2,"a":["x",1e2,-0]}""",
                ],
                first.Values.Select(value => value.CanonicalText));
            Assert.All(first.Values, value => Assert.True(value.BinaryValue.IsEmpty));
            Assert.Null(first.StableKey);

            MigrationSourceValue mixedString = batch.Rows[1].Values[5];
            Assert.Equal(MigrationSourceValueKind.Text, mixedString.Kind);
            Assert.Equal("\"line\\n\"", mixedString.CanonicalText);
        }
    }

    [Fact]
    public async Task RepeatedReadsPreserveArbitraryProjectionAndStableRows()
    {
        string JsonObject(string prefix) =>
            "{" +
            string.Join(
                ',',
                Enumerable.Range(0, 12)
                    .Select(index => $"\"c{index}\":\"{prefix}-{index}\"")) +
            "}";
        string json = $"[{JsonObject("first")},{JsonObject("second")}]";
        (JsonSourceSnapshot snapshot, JsonTableSchemaInferenceResult schema) =
            await InferAsync(json);
        await using (snapshot)
        await using (JsonMigrationDataSource source =
                     await JsonMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            int[] projection = [10, 2, 11, 0, 9, 1, 8, 3, 7, 4, 6, 5];
            string[] columnIds = projection
                .Select(JsonMigrationObjectIds.Column)
                .ToArray();
            MigrationReadRequest request =
                Request(source, columnIds, batchSize: 1);

            List<MigrationDataBatch> first =
                await CollectAsync(source.ReadAsync(request, Cancellation));
            List<MigrationDataBatch> repeated =
                await CollectAsync(source.ReadAsync(request, Cancellation));

            Assert.Equal(2, first.Count);
            Assert.Equal(first.Count, repeated.Count);
            for (int index = 0; index < first.Count; index++)
                AssertBatchEqual(first[index], repeated[index]);

            Assert.Equal(columnIds, first[0].ColumnObjectIds);
            Assert.Equal(
                projection.Select(index => $"first-{index}"),
                TextValues(Assert.Single(first[0].Rows)));
            Assert.Equal(
                projection.Select(index => $"second-{index}"),
                TextValues(Assert.Single(first[1].Rows)));
            Assert.All(
                first.SelectMany(batch => batch.Rows),
                row => Assert.Null(row.StableKey));
        }
    }

    [Fact]
    public async Task MissingAsNullEmitsNullWhileStrictMissingFailsAtExactColumn()
    {
        var asNullOptions = new JsonTableSchemaInferenceOptions
        {
            ColumnOverrides =
            [
                new JsonTableColumnSchemaOverride
                {
                    ColumnIndex = 0,
                    ExpectedPropertyName = "optional",
                    LogicalType = JsonTableColumnLogicalType.SignedInteger,
                    MissingPolicy = JsonMissingPropertyPolicy.AsNull,
                },
            ],
        };
        (JsonSourceSnapshot asNullSnapshot, JsonTableSchemaInferenceResult asNullSchema) =
            await InferAsync(
                """[{"optional":1,"required":"present"},{"required":"next"}]""",
                options: asNullOptions);
        await using (asNullSnapshot)
        await using (JsonMigrationDataSource source =
                     await JsonMigrationDataSource.CreateAsync(
                         asNullSchema,
                         asNullSnapshot,
                         Catalog(asNullSchema),
                         Cancellation))
        {
            MigrationDataBatch batch = Assert.Single(await CollectAsync(
                source.ReadAsync(
                    Request(
                        source,
                        [
                            JsonMigrationObjectIds.Column(0),
                            JsonMigrationObjectIds.Column(1),
                        ],
                        batchSize: 10),
                    Cancellation)));
            Assert.Equal(2, batch.Rows.Count);
            Assert.Equal(
                MigrationSourceValueKind.SignedInteger,
                batch.Rows[0].Values[0].Kind);
            Assert.Equal(
                MigrationSourceValueKind.Null,
                batch.Rows[1].Values[0].Kind);
            Assert.Null(batch.Rows[1].Values[0].CanonicalText);
            Assert.Equal("next", batch.Rows[1].Values[1].CanonicalText);
        }

        (JsonSourceSnapshot strictSnapshot, JsonTableSchemaInferenceResult strictSchema) =
            await InferAsync("""[{"required":"present"},{}]""");
        await using (strictSnapshot)
        await using (JsonMigrationDataSource strictSource =
                     await JsonMigrationDataSource.CreateAsync(
                         strictSchema,
                         strictSnapshot,
                         Catalog(strictSchema),
                         Cancellation))
        {
            MigrationReadRequest request = Request(
                strictSource,
                [JsonMigrationObjectIds.Column(0)],
                batchSize: 1);
            await using IAsyncEnumerator<MigrationDataBatch> records = strictSource
                .ReadAsync(request, Cancellation)
                .GetAsyncEnumerator(Cancellation);
            Assert.True(await records.MoveNextAsync());
            Assert.Equal(
                "present",
                Assert.Single(
                    Assert.Single(records.Current.Rows).Values).CanonicalText);

            MigrationRowRejectedException error =
                await Assert.ThrowsAsync<MigrationRowRejectedException>(
                    async () => await records.MoveNextAsync());
            Assert.Equal(JsonMigrationDataRules.MissingProperty, error.Code);
            Assert.Equal(JsonMigrationObjectIds.Column(0), error.ColumnObjectId);
            Assert.Equal(1, error.BatchOrdinal);
            Assert.Equal(1, error.SourceRowOrdinal);
        }
    }

    [Fact]
    public async Task LateTypeNullAndNonObjectFailuresUseStableFailFastRules()
    {
        const string secret = "TOP-SECRET-LATE-TYPE";
        (JsonSourceSnapshot typeSnapshot, JsonTableSchemaInferenceResult typeSchema) =
            await InferAsync(
                $"[{{\"value\":1}},{{\"value\":\"{secret}\"}}]",
                maxProfileRecords: 1);
        await using (typeSnapshot)
        await using (JsonMigrationDataSource typeSource =
                     await JsonMigrationDataSource.CreateAsync(
                         typeSchema,
                         typeSnapshot,
                         Catalog(typeSchema),
                         Cancellation))
        {
            MigrationRowRejectedException error = await ReadFirstBatchThenFailureAsync(
                typeSource,
                Request(
                    typeSource,
                    [JsonMigrationObjectIds.Column(0)],
                    batchSize: 1));
            Assert.Equal(JsonMigrationDataRules.TypeMismatch, error.Code);
            Assert.Equal(JsonMigrationObjectIds.Column(0), error.ColumnObjectId);
            Assert.Equal(1, error.BatchOrdinal);
            Assert.Equal(1, error.SourceRowOrdinal);
            Assert.DoesNotContain(secret, error.ToString(), StringComparison.Ordinal);
        }

        var requiredOptions = new JsonTableSchemaInferenceOptions
        {
            ColumnOverrides =
            [
                new JsonTableColumnSchemaOverride
                {
                    ColumnIndex = 0,
                    ExpectedPropertyName = "value",
                    LogicalType = JsonTableColumnLogicalType.SignedInteger,
                    Nullable = false,
                },
            ],
        };
        (JsonSourceSnapshot nullSnapshot, JsonTableSchemaInferenceResult nullSchema) =
            await InferAsync(
                """[{"value":1},{"value":null}]""",
                options: requiredOptions);
        await using (nullSnapshot)
        await using (JsonMigrationDataSource nullSource =
                     await JsonMigrationDataSource.CreateAsync(
                         nullSchema,
                         nullSnapshot,
                         Catalog(nullSchema),
                         Cancellation))
        {
            MigrationRowRejectedException error = await ReadFirstBatchThenFailureAsync(
                nullSource,
                Request(
                    nullSource,
                    [JsonMigrationObjectIds.Column(0)],
                    batchSize: 1));
            Assert.Equal(JsonMigrationDataRules.NullNotAllowed, error.Code);
            Assert.Equal(JsonMigrationObjectIds.Column(0), error.ColumnObjectId);
            Assert.Equal(1, error.SourceRowOrdinal);
        }

        (JsonSourceSnapshot rowSnapshot, JsonTableSchemaInferenceResult rowSchema) =
            await InferAsync("""[{"value":1},42]""");
        await using (rowSnapshot)
        await using (JsonMigrationDataSource rowSource =
                     await JsonMigrationDataSource.CreateAsync(
                         rowSchema,
                         rowSnapshot,
                         Catalog(rowSchema),
                         Cancellation))
        {
            MigrationRowRejectedException error = await ReadFirstBatchThenFailureAsync(
                rowSource,
                Request(
                    rowSource,
                    [JsonMigrationObjectIds.Column(0)],
                    batchSize: 1));
            Assert.Equal(JsonMigrationDataRules.NonObjectRow, error.Code);
            Assert.Equal(JsonMigrationObjectIds.Table, error.ColumnObjectId);
            Assert.Equal(1, error.BatchOrdinal);
            Assert.Equal(1, error.SourceRowOrdinal);
        }
    }

    [Fact]
    public async Task DeterministicRejectsCoverEveryRowRuleAndFreezeGoldenEvidence()
    {
        const string json =
            """[{"id":1},42,{"id":"bad"},{},{"id":null}]""";
        var schemaOptions = new JsonTableSchemaInferenceOptions
        {
            ColumnOverrides =
            [
                new JsonTableColumnSchemaOverride
                {
                    ColumnIndex = 0,
                    ExpectedPropertyName = "id",
                    LogicalType = JsonTableColumnLogicalType.SignedInteger,
                    Nullable = false,
                },
            ],
        };
        (JsonSourceSnapshot snapshot, JsonTableSchemaInferenceResult schema) =
            await InferAsync(
                json,
                maxProfileRecords: 1,
                options: schemaOptions);
        await using (snapshot)
        await using (JsonMigrationDataSource source =
                     await JsonMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            Assert.Equal(
                MigrationRejectContract.DeterministicRejectsV1,
                source.RejectContractVersion);
            Assert.Equal(
                [
                    JsonMigrationDataRules.MissingProperty,
                    JsonMigrationDataRules.NullNotAllowed,
                    JsonMigrationDataRules.NonObjectRow,
                    JsonMigrationDataRules.TypeMismatch,
                ],
                source.SupportedRejectRuleIds.Order(StringComparer.Ordinal));

            MigrationDataBatch batch = Assert.Single(await CollectAsync(
                source.ReadAsync(
                    RejectRequest(
                        source,
                        [JsonMigrationObjectIds.Column(0)],
                        batchSize: 5),
                    Cancellation)));
            Assert.Single(batch.Rows);
            Assert.Equal("1", Assert.Single(batch.Rows[0].Values).CanonicalText);
            Assert.Equal(
                [1L, 2L, 3L, 4L],
                batch.RejectedRows.Select(row => row.SourceRowOrdinal));
            Assert.Equal(
                [
                    JsonMigrationDataRules.NonObjectRow,
                    JsonMigrationDataRules.TypeMismatch,
                    JsonMigrationDataRules.MissingProperty,
                    JsonMigrationDataRules.NullNotAllowed,
                ],
                batch.RejectedRows.Select(row => row.RuleId));

            MigrationRejectedRow nonObject = batch.RejectedRows[0];
            Assert.Null(nonObject.ColumnObjectId);
            AssertGoldenEvidence(
                nonObject,
                [
                    "jsonValueKind",
                    "rawValue",
                    "recordByteLength",
                    "recordOrdinal",
                    "startByteOffset",
                    "startBytePositionInLine",
                    "startLineNumber",
                ],
                ["Number", "42", "2", "2", "10", "10", "1"]);

            MigrationRejectedRow type = batch.RejectedRows[1];
            Assert.Equal(JsonMigrationObjectIds.Column(0), type.ColumnObjectId);
            AssertGoldenEvidence(
                type,
                ColumnEvidenceNames,
                ["0", "String", "0", "\"bad\"", "12", "3", "13", "13", "1"]);

            MigrationRejectedRow missing = batch.RejectedRows[2];
            AssertGoldenEvidence(
                missing,
                ColumnEvidenceNames,
                ["0", "Missing", null, null, "2", "4", "26", "26", "1"]);

            MigrationRejectedRow explicitNull = batch.RejectedRows[3];
            AssertGoldenEvidence(
                explicitNull,
                ColumnEvidenceNames,
                ["0", "Null", "0", "null", "11", "5", "29", "29", "1"]);
            Assert.Null(batch.NextCursor);
        }
    }

    [Fact]
    public async Task ValueAndRowSizeFailuresRemainFatalInRejectMode()
    {
        (JsonSourceSnapshot valueSnapshot, JsonTableSchemaInferenceResult valueSchema) =
            await InferAsync("""[{"value":"😀"}]""");
        await using (valueSnapshot)
        await using (JsonMigrationDataSource valueSource =
                     await JsonMigrationDataSource.CreateAsync(
                         valueSchema,
                         valueSnapshot,
                         Catalog(valueSchema),
                         Cancellation))
        {
            MigrationRowRejectedException error =
                await Assert.ThrowsAsync<MigrationRowRejectedException>(
                    async () => await CollectAsync(valueSource.ReadAsync(
                        RejectRequest(
                            valueSource,
                            [JsonMigrationObjectIds.Column(0)],
                            batchSize: 1,
                            maxBatchBytes: 18,
                            maxValueBytes: 8),
                        Cancellation)));
            Assert.Equal(JsonMigrationDataRules.ValueSizeExceeded, error.Code);
            Assert.Equal(0, error.BatchOrdinal);
            Assert.Equal(0, error.SourceRowOrdinal);
        }

        (JsonSourceSnapshot rowSnapshot, JsonTableSchemaInferenceResult rowSchema) =
            await InferAsync("""[{"left":"😀","right":"😀"}]""");
        await using (rowSnapshot)
        await using (JsonMigrationDataSource rowSource =
                     await JsonMigrationDataSource.CreateAsync(
                         rowSchema,
                         rowSnapshot,
                         Catalog(rowSchema),
                         Cancellation))
        {
            MigrationRowRejectedException error =
                await Assert.ThrowsAsync<MigrationRowRejectedException>(
                    async () => await CollectAsync(rowSource.ReadAsync(
                        RejectRequest(
                            rowSource,
                            [
                                JsonMigrationObjectIds.Column(0),
                                JsonMigrationObjectIds.Column(1),
                            ],
                            batchSize: 1,
                            maxBatchBytes: 17,
                            maxValueBytes: 9),
                        Cancellation)));
            Assert.Equal(JsonMigrationDataRules.RowSizeExceeded, error.Code);
            Assert.Equal(JsonMigrationObjectIds.Column(1), error.ColumnObjectId);
            Assert.Equal(0, error.SourceRowOrdinal);
        }
    }

    [Fact]
    public async Task RejectLimitsFailFatallyWithoutLeakingRawValues()
    {
        const string firstSecret = "TOP-SECRET-ONE";
        const string secondSecret = "TOP-SECRET-TWO";
        string json =
            $"[{{\"id\":1}},{{\"id\":\"{firstSecret}\"}},{{\"id\":\"{secondSecret}\"}}]";
        (JsonSourceSnapshot snapshot, JsonTableSchemaInferenceResult schema) =
            await InferAsync(json, maxProfileRecords: 1);
        await using (snapshot)
        await using (JsonMigrationDataSource source =
                     await JsonMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            MigrationReadRequest request = RejectRequest(
                source,
                [JsonMigrationObjectIds.Column(0)],
                batchSize: 2);
            MigrationDeterministicRejectPolicy policy = request.RejectPolicy!;
            MigrationDeterministicRejectPolicy[] limitedPolicies =
            [
                policy with { MaxRawValueBytes = 1 },
                policy with
                {
                    MaxRejectedRowsPerBatch = 1,
                    MaxRejectedRowsPerRun = 1,
                },
                policy with
                {
                    MaxArtifactBytes =
                        MigrationRejectLedgerCodec.MinimumCanonicalArtifactBytes,
                },
            ];

            foreach (MigrationDeterministicRejectPolicy limited in limitedPolicies)
            {
                InvalidDataException error =
                    await Assert.ThrowsAsync<InvalidDataException>(
                        async () => await CollectAsync(source.ReadAsync(
                            request with { RejectPolicy = limited },
                            Cancellation)));
                Assert.DoesNotContain(
                    firstSecret,
                    error.ToString(),
                    StringComparison.Ordinal);
                Assert.DoesNotContain(
                    secondSecret,
                    error.ToString(),
                    StringComparison.Ordinal);
            }
        }
    }

    [Fact]
    public async Task RejectBudgetsSplitAndEnforceExactRunAndArtifactBounds()
    {
        const string json =
            """[{"id":1},{"id":"x"},{"id":"y"},{"id":4}]""";
        (JsonSourceSnapshot snapshot, JsonTableSchemaInferenceResult schema) =
            await InferAsync(json, maxProfileRecords: 1);
        await using (snapshot)
        await using (JsonMigrationDataSource source =
                     await JsonMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            MigrationReadRequest request = RejectRequest(
                source,
                [JsonMigrationObjectIds.Column(0)],
                batchSize: 4);
            List<MigrationDataBatch> baseline =
                await CollectAsync(source.ReadAsync(request, Cancellation));
            MigrationDataBatch baselineBatch = Assert.Single(baseline);
            Assert.Equal(2, baselineBatch.RejectedRows.Count);
            int[] rawBytes = baselineBatch.RejectedRows
                .Select(MigrationRejectLedgerCodec.GetRawValueByteCount)
                .ToArray();
            int maximumRawBytes = rawBytes.Max();
            int totalRawBytes = rawBytes.Sum();
            long exactArtifactBytes = checked(
                MigrationRejectLedgerCodec.MinimumCanonicalArtifactBytes +
                baselineBatch.RejectedRows.Sum(row =>
                    (long)MigrationRejectLedgerCodec
                        .GetCanonicalArtifactEntryByteCount(
                            JsonMigrationObjectIds.Table,
                            baselineBatch.BatchOrdinal,
                            row)));
            MigrationDeterministicRejectPolicy policy =
                request.RejectPolicy!;

            MigrationReadRequest countSplitRequest = request with
            {
                RejectPolicy = policy with
                {
                    MaxRejectedRowsPerBatch = 1,
                },
            };
            List<MigrationDataBatch> countSplit = await CollectAsync(
                source.ReadAsync(countSplitRequest, Cancellation));
            Assert.Equal([2, 2], countSplit.Select(OutcomeCount));
            string cursor =
                Assert.IsType<string>(countSplit[0].NextCursor);
            MigrationDataBatch resumed = Assert.Single(await CollectAsync(
                source.ReadAsync(
                    countSplitRequest with { ResumeCursor = cursor },
                    Cancellation)));
            AssertBatchEqual(countSplit[1], resumed);

            List<MigrationDataBatch> rawSplit = await CollectAsync(
                source.ReadAsync(
                    request with
                    {
                        RejectPolicy = policy with
                        {
                            MaxRawValueBytes = maximumRawBytes,
                            MaxRawValueBytesPerBatch = maximumRawBytes,
                            MaxRawValueBytesPerRun = totalRawBytes,
                        },
                    },
                    Cancellation));
            Assert.Equal([2, 2], rawSplit.Select(OutcomeCount));

            MigrationReadRequest exactRunRequest = request with
            {
                RejectPolicy = policy with
                {
                    MaxRawValueBytes = maximumRawBytes,
                    MaxRawValueBytesPerBatch = totalRawBytes,
                    MaxRawValueBytesPerRun = totalRawBytes,
                    MaxArtifactBytes = exactArtifactBytes,
                },
            };
            Assert.Single(await CollectAsync(
                source.ReadAsync(exactRunRequest, Cancellation)));

            await Assert.ThrowsAsync<InvalidDataException>(
                async () => await CollectAsync(source.ReadAsync(
                    exactRunRequest with
                    {
                        RejectPolicy = exactRunRequest.RejectPolicy! with
                        {
                            MaxRawValueBytesPerBatch =
                                totalRawBytes - 1,
                            MaxRawValueBytesPerRun =
                                totalRawBytes - 1,
                        },
                    },
                    Cancellation)));
            await Assert.ThrowsAsync<InvalidDataException>(
                async () => await CollectAsync(source.ReadAsync(
                    exactRunRequest with
                    {
                        RejectPolicy = exactRunRequest.RejectPolicy! with
                        {
                            MaxArtifactBytes =
                                exactArtifactBytes - 1,
                        },
                    },
                    Cancellation)));
        }
    }

    [Fact]
    public async Task RowAndCanonicalByteBoundsSplitBeforeOverflow()
    {
        const string json =
            """[{"value":""},{"value":""},{"value":""},{"value":""},{"value":""},{"value":""},{"value":""}]""";
        (JsonSourceSnapshot snapshot, JsonTableSchemaInferenceResult schema) =
            await InferAsync(json);
        await using (snapshot)
        await using (JsonMigrationDataSource source =
                     await JsonMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            List<MigrationDataBatch> rowBound = await CollectAsync(
                source.ReadAsync(
                    Request(
                        source,
                        [JsonMigrationObjectIds.Column(0)],
                        batchSize: 3,
                        maxBatchBytes: 1_024,
                        maxValueBytes: 5),
                    Cancellation));
            Assert.Equal([3, 3, 1], rowBound.Select(batch => batch.Rows.Count));

            List<MigrationDataBatch> byteBound = await CollectAsync(
                source.ReadAsync(
                    Request(
                        source,
                        [JsonMigrationObjectIds.Column(0)],
                        batchSize: 10,
                        maxBatchBytes: 10,
                        maxValueBytes: 5),
                    Cancellation));
            Assert.Equal([2, 2, 2, 1], byteBound.Select(batch => batch.Rows.Count));
            Assert.All(
                byteBound.SelectMany(batch => batch.Rows),
                row => Assert.Equal(
                    string.Empty,
                    Assert.Single(row.Values).CanonicalText));
            Assert.Null(byteBound[^1].NextCursor);
        }
    }

    [Fact]
    public async Task JsonSpecificValueAndRowByteBoundsHonorExactEquality()
    {
        const string json =
            """
            [
              {
                "payload":{"quote":"a\\b","emoji":"😀"},
                "number":1,
                "nullable":null
              }
            ]
            """;
        var options = new JsonTableSchemaInferenceOptions
        {
            ColumnOverrides =
            [
                new JsonTableColumnSchemaOverride
                {
                    ColumnIndex = 0,
                    ExpectedPropertyName = "payload",
                    LogicalType = JsonTableColumnLogicalType.Json,
                    Nullable = false,
                },
                new JsonTableColumnSchemaOverride
                {
                    ColumnIndex = 1,
                    ExpectedPropertyName = "number",
                    LogicalType =
                        JsonTableColumnLogicalType.SignedInteger,
                    Nullable = false,
                },
                new JsonTableColumnSchemaOverride
                {
                    ColumnIndex = 2,
                    ExpectedPropertyName = "nullable",
                    LogicalType = JsonTableColumnLogicalType.Text,
                    Nullable = true,
                },
            ],
        };
        (JsonSourceSnapshot snapshot, JsonTableSchemaInferenceResult schema) =
            await InferAsync(json, options: options);
        await using (snapshot)
        await using (JsonMigrationDataSource source =
                     await JsonMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            const string canonicalJson =
                """{"quote":"a\\b","emoji":"😀"}""";
            int jsonValueBytes =
                checked(5 + Encoding.UTF8.GetByteCount(canonicalJson));
            int exactRowBytes = checked(jsonValueBytes + 9 + 1);
            string[] columns =
            [
                JsonMigrationObjectIds.Column(0),
                JsonMigrationObjectIds.Column(1),
                JsonMigrationObjectIds.Column(2),
            ];
            MigrationReadRequest exact = Request(
                source,
                columns,
                batchSize: 1,
                maxBatchBytes: exactRowBytes,
                maxValueBytes: jsonValueBytes);

            MigrationDataBatch batch = Assert.Single(await CollectAsync(
                source.ReadAsync(exact, Cancellation)));
            MigrationDataRow row = Assert.Single(batch.Rows);
            Assert.Equal(canonicalJson, row.Values[0].CanonicalText);
            Assert.Equal("1", row.Values[1].CanonicalText);
            Assert.Equal(MigrationSourceValueKind.Null, row.Values[2].Kind);

            MigrationRowRejectedException valueError =
                await Assert.ThrowsAsync<MigrationRowRejectedException>(
                    async () => await CollectAsync(source.ReadAsync(
                        exact with
                        {
                            MaxValueBytes = jsonValueBytes - 1,
                        },
                        Cancellation)));
            Assert.Equal(
                JsonMigrationDataRules.ValueSizeExceeded,
                valueError.Code);

            MigrationRowRejectedException rowError =
                await Assert.ThrowsAsync<MigrationRowRejectedException>(
                    async () => await CollectAsync(source.ReadAsync(
                        exact with
                        {
                            MaxBatchBytes = exactRowBytes - 1,
                        },
                        Cancellation)));
            Assert.Equal(
                JsonMigrationDataRules.RowSizeExceeded,
                rowError.Code);
            Assert.Equal(
                JsonMigrationObjectIds.Column(2),
                rowError.ColumnObjectId);
        }
    }

    [Fact]
    public async Task CursorChainResumesEveryExactSuffixAndEndsAtEof()
    {
        string json =
            "[" +
            string.Join(
                ',',
                Enumerable.Range(1, 7)
                    .Select(value => $"{{\"id\":{value}}}")) +
            "]";
        (JsonSourceSnapshot snapshot, JsonTableSchemaInferenceResult schema) =
            await InferAsync(json);
        await using (snapshot)
        await using (JsonMigrationDataSource source =
                     await JsonMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            MigrationReadRequest request = Request(
                source,
                [JsonMigrationObjectIds.Column(0)],
                batchSize: 3);
            List<MigrationDataBatch> batches =
                await CollectAsync(source.ReadAsync(request, Cancellation));

            Assert.Equal([3, 3, 1], batches.Select(batch => batch.Rows.Count));
            Assert.Equal([0L, 1L, 2L], batches.Select(batch => batch.BatchOrdinal));
            Assert.Null(batches[0].StartCursor);
            Assert.Equal(batches[0].NextCursor, batches[1].StartCursor);
            Assert.Equal(batches[1].NextCursor, batches[2].StartCursor);
            Assert.Null(batches[2].NextCursor);
            Assert.StartsWith(
                JsonMigrationDataSource.CursorAlgorithmId + "/3/1/",
                batches[0].NextCursor,
                StringComparison.Ordinal);
            Assert.Equal(
                64,
                Assert.IsType<string>(batches[0].NextCursor)
                    .Split('/')[^1].Length);

            for (int boundary = 0; boundary < batches.Count - 1; boundary++)
            {
                string cursor =
                    Assert.IsType<string>(batches[boundary].NextCursor);
                List<MigrationDataBatch> resumed = await CollectAsync(
                    source.ReadAsync(
                        request with { ResumeCursor = cursor },
                        Cancellation));
                Assert.Equal(
                    batches.Count - boundary - 1,
                    resumed.Count);
                for (int suffix = 0; suffix < resumed.Count; suffix++)
                {
                    AssertBatchEqual(
                        batches[boundary + suffix + 1],
                        resumed[suffix]);
                }
            }
        }
    }

    [Fact]
    public async Task MultipleValueReadsCanReplayConcurrently()
    {
        const string json =
            """
            {"id":1}
            {"id":2}
            {"id":3}
            """;
        (JsonSourceSnapshot snapshot, JsonTableSchemaInferenceResult schema) =
            await InferAsync(
                json,
                framing: JsonInputFraming.MultipleValues);
        await using (snapshot)
        await using (JsonMigrationDataSource source =
                     await JsonMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            MigrationReadRequest request = Request(
                source,
                [JsonMigrationObjectIds.Column(0)],
                batchSize: 2);
            Task<List<MigrationDataBatch>> first =
                CollectAsync(source.ReadAsync(request, Cancellation));
            Task<List<MigrationDataBatch>> second =
                CollectAsync(source.ReadAsync(request, Cancellation));

            List<MigrationDataBatch>[] results =
                await Task.WhenAll(first, second);
            List<MigrationDataBatch> firstResult = results[0];
            List<MigrationDataBatch> secondResult = results[1];

            Assert.Equal(2, firstResult.Count);
            Assert.Equal(firstResult.Count, secondResult.Count);
            for (int index = 0; index < firstResult.Count; index++)
                AssertBatchEqual(firstResult[index], secondResult[index]);
            Assert.Equal(
                ["1", "2", "3"],
                firstResult
                    .SelectMany(batch => batch.Rows)
                    .Select(row =>
                        Assert.Single(row.Values).CanonicalText));
        }
    }

    [Fact]
    public async Task CursorScopeBindsProjectionLimitsAndFullRejectPolicy()
    {
        const string json =
            """[{"id":1,"other":"a"},{"id":"bad","other":"b"},{"id":3,"other":"c"}]""";
        (JsonSourceSnapshot snapshot, JsonTableSchemaInferenceResult schema) =
            await InferAsync(json, maxProfileRecords: 1);
        await using (snapshot)
        await using (JsonMigrationDataSource source =
                     await JsonMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            MigrationReadRequest request = RejectRequest(
                source,
                [
                    JsonMigrationObjectIds.Column(0),
                    JsonMigrationObjectIds.Column(1),
                ],
                batchSize: 2);
            List<MigrationDataBatch> batches =
                await CollectAsync(source.ReadAsync(request, Cancellation));
            Assert.Equal([2, 1], batches.Select(OutcomeCount));
            string cursor = Assert.IsType<string>(batches[0].NextCursor);

            MigrationDeterministicRejectPolicy policy = request.RejectPolicy!;
            List<MigrationDataBatch> reordered = await CollectAsync(
                source.ReadAsync(
                    request with
                    {
                        ResumeCursor = cursor,
                        RejectPolicy = policy with
                        {
                            AllowedRuleIds =
                                policy.AllowedRuleIds.Reverse().ToArray(),
                        },
                    },
                    Cancellation));
            Assert.Single(reordered);
            AssertBatchEqual(batches[1], reordered[0]);

            char replacement = cursor[^1] == '0' ? '1' : '0';
            string tampered = cursor[..^1] + replacement;
            MigrationReadRequest[] drifted =
            [
                request with
                {
                    RejectContractVersion =
                        MigrationRejectContract.DeterministicFailFastV1,
                    RejectPolicy = null,
                },
                request with
                {
                    ColumnObjectIds =
                    [
                        JsonMigrationObjectIds.Column(1),
                        JsonMigrationObjectIds.Column(0),
                    ],
                },
                request with { BatchSize = 3 },
                request with
                {
                    MaxBatchBytes = request.MaxBatchBytes - 1,
                },
                request with
                {
                    MaxValueBytes = request.MaxValueBytes - 1,
                },
                request with
                {
                    RejectPolicy = policy with
                    {
                        AllowedRuleIds =
                        [
                            JsonMigrationDataRules.TypeMismatch,
                        ],
                    },
                },
                request with
                {
                    RejectPolicy = policy with
                    {
                        MaxRejectedRowsPerBatch =
                            policy.MaxRejectedRowsPerBatch - 1,
                    },
                },
                request with
                {
                    RejectPolicy = policy with
                    {
                        MaxRejectedRowsPerRun =
                            policy.MaxRejectedRowsPerRun - 1,
                    },
                },
                request with
                {
                    RejectPolicy = policy with
                    {
                        MaxRawValueBytes =
                            policy.MaxRawValueBytes - 1,
                    },
                },
                request with
                {
                    RejectPolicy = policy with
                    {
                        MaxRawValueBytesPerBatch =
                            policy.MaxRawValueBytesPerBatch - 1,
                    },
                },
                request with
                {
                    RejectPolicy = policy with
                    {
                        MaxRawValueBytesPerRun =
                            policy.MaxRawValueBytesPerRun - 1,
                    },
                },
                request with
                {
                    RejectPolicy = policy with
                    {
                        MaxArtifactBytes =
                            policy.MaxArtifactBytes - 1,
                    },
                },
            ];
            foreach (MigrationReadRequest driftedRequest in drifted)
            {
                Assert.Throws<InvalidDataException>(() =>
                    source.ReadAsync(
                        driftedRequest with { ResumeCursor = cursor },
                        Cancellation));
            }

            Assert.Throws<InvalidDataException>(() => source.ReadAsync(
                request with
                {
                    ResumeCursor = cursor,
                    SnapshotToken = null,
                },
                Cancellation));
            Assert.Throws<InvalidDataException>(() => source.ReadAsync(
                request with { ResumeCursor = tampered },
                Cancellation));
            Assert.Throws<InvalidDataException>(() => source.ReadAsync(
                request with { SnapshotToken = "wrong-snapshot" },
                Cancellation));
        }
    }

    [Fact]
    public async Task CancellationDoesNotPoisonLaterReplay()
    {
        const string json =
            """[{"id":1},{"id":2},{"id":3}]""";
        (JsonSourceSnapshot snapshot, JsonTableSchemaInferenceResult schema) =
            await InferAsync(json);
        await using (snapshot)
        await using (JsonMigrationDataSource source =
                     await JsonMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            MigrationReadRequest request = Request(
                source,
                [JsonMigrationObjectIds.Column(0)],
                batchSize: 1);
            using (var preCanceled =
                   CancellationTokenSource.CreateLinkedTokenSource(Cancellation))
            {
                await preCanceled.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(
                    async () => await CollectWithTokenAsync(
                        source.ReadAsync(request, preCanceled.Token),
                        preCanceled.Token));
            }

            using (var interrupted =
                   CancellationTokenSource.CreateLinkedTokenSource(Cancellation))
            {
                await using IAsyncEnumerator<MigrationDataBatch> records = source
                    .ReadAsync(request, interrupted.Token)
                    .GetAsyncEnumerator(interrupted.Token);
                Assert.True(await records.MoveNextAsync());
                Assert.Equal(
                    "1",
                    Assert.Single(
                        Assert.Single(records.Current.Rows).Values).CanonicalText);
                await interrupted.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(
                    async () => await records.MoveNextAsync());
            }

            List<MigrationDataBatch> replay =
                await CollectAsync(source.ReadAsync(request, Cancellation));
            Assert.Equal(
                ["1", "2", "3"],
                replay.Select(batch =>
                    Assert.Single(
                        Assert.Single(batch.Rows).Values).CanonicalText));
        }
    }

    [Fact]
    public async Task DisposalIsIdempotentAndSnapshotRemainsCallerOwned()
    {
        const string json = """[{"id":1},{"id":2}]""";
        (JsonSourceSnapshot snapshot, JsonTableSchemaInferenceResult schema) =
            await InferAsync(json);
        await using (snapshot)
        {
            JsonMigrationDataSource source =
                await JsonMigrationDataSource.CreateAsync(
                    schema,
                    snapshot,
                    Catalog(schema),
                    Cancellation);
            MigrationReadRequest request = Request(
                source,
                [JsonMigrationObjectIds.Column(0)],
                batchSize: 2);

            await source.DisposeAsync();
            await source.DisposeAsync();
            Assert.Throws<ObjectDisposedException>(() =>
                source.ReadAsync(request, Cancellation));

            await using Stream snapshotReader = snapshot.OpenRead();
            Assert.True(snapshotReader.CanRead);

            await using JsonMigrationDataSource replacement =
                await JsonMigrationDataSource.CreateAsync(
                    schema,
                    snapshot,
                    Catalog(schema),
                    Cancellation);
            MigrationDataBatch batch = Assert.Single(await CollectAsync(
                replacement.ReadAsync(
                    Request(
                        replacement,
                        [JsonMigrationObjectIds.Column(0)],
                        batchSize: 2),
                    Cancellation)));
            Assert.Equal(2, batch.Rows.Count);
        }
    }

    [Fact]
    public async Task ReadsDetectPrivateSnapshotTampering()
    {
        const string json = """[{"id":1},{"id":2}]""";
        (JsonSourceSnapshot snapshot, JsonTableSchemaInferenceResult schema) =
            await InferAsync(json);
        try
        {
            await using JsonMigrationDataSource source =
                await JsonMigrationDataSource.CreateAsync(
                    schema,
                    snapshot,
                    Catalog(schema),
                    Cancellation);
            string snapshotPath = Assert.IsType<string>(
                typeof(JsonSourceSnapshot)
                    .GetField(
                        "snapshotPath",
                        BindingFlags.Instance | BindingFlags.NonPublic)!
                    .GetValue(snapshot));
            FileStream guard = Assert.IsType<FileStream>(
                typeof(JsonSourceSnapshot)
                    .GetField(
                        "integrityGuard",
                        BindingFlags.Instance | BindingFlags.NonPublic)!
                    .GetValue(snapshot));
            await guard.DisposeAsync();
            await File.WriteAllBytesAsync(
                snapshotPath,
                Utf8Bytes("""[{"id":9},{"id":2}]"""),
                Cancellation);

            JsonSourceSnapshotException error =
                await Assert.ThrowsAsync<JsonSourceSnapshotException>(
                    async () => await CollectAsync(source.ReadAsync(
                        Request(
                            source,
                            [JsonMigrationObjectIds.Column(0)],
                            batchSize: 2),
                        Cancellation)));
            Assert.Equal(
                JsonSnapshotDiagnosticRules.IntegrityMismatch,
                error.RuleId);
        }
        finally
        {
            await snapshot.DisposeAsync();
        }
    }

    private static readonly string[] ColumnEvidenceNames =
    [
        "columnIndex",
        "jsonValueKind",
        "propertyOrdinal",
        MigrationRejectLedgerCodec.RawValueEvidenceName,
        "recordByteLength",
        "recordOrdinal",
        "startByteOffset",
        "startBytePositionInLine",
        "startLineNumber",
    ];

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    private static MigrationCatalog Catalog(
        JsonTableSchemaInferenceResult schema) =>
        schema.CreateCatalog(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);

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
                ],
                MaxRejectedRowsPerBatch = batchSize,
                MaxRejectedRowsPerRun = 1_000,
                MaxRawValueBytes = 4_096,
                MaxRawValueBytesPerBatch = 64 * 1_024,
                MaxRawValueBytesPerRun = 1024 * 1_024,
                MaxArtifactBytes = 16 * 1_024 * 1_024,
            },
        };

    private static int OutcomeCount(MigrationDataBatch batch) =>
        checked(batch.Rows.Count + batch.RejectedRows.Count);

    private static IEnumerable<string?> TextValues(MigrationDataRow row) =>
        row.Values.Select(value => value.CanonicalText);

    private static void AssertGoldenEvidence(
        MigrationRejectedRow rejectedRow,
        IReadOnlyList<string> expectedNames,
        IReadOnlyList<string?> expectedValues)
    {
        Assert.Equal(
            expectedNames,
            rejectedRow.Evidence.Select(item => item.Name));
        Assert.Equal(
            expectedValues,
            rejectedRow.Evidence.Select(item => item.Value));
    }

    private static async ValueTask<(
        JsonSourceSnapshot Snapshot,
        JsonTableSchemaInferenceResult Schema)> InferAsync(
        string json,
        int maxProfileRecords = 1_000,
        JsonTableSchemaInferenceOptions? options = null,
        JsonInputFraming framing = JsonInputFraming.RootArray)
    {
        JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateAsync(
                new MemoryStream(Utf8Bytes(json)),
                cancellationToken: Cancellation);
        try
        {
            JsonSourceBinding binding =
                await JsonSourceBinding.CreateAsync(
                    snapshot,
                    new JsonStreamingReaderOptions
                    {
                        Framing = framing,
                    },
                    cancellationToken: Cancellation);
            JsonTableSchemaInferenceResult schema =
                await JsonTableSchemaInferer.InferAsync(
                    binding,
                    snapshot,
                    maxProfileRecords,
                    options,
                    Cancellation);
            return (snapshot, schema);
        }
        catch
        {
            await snapshot.DisposeAsync();
            throw;
        }
    }

    private static async Task<MigrationRowRejectedException>
        ReadFirstBatchThenFailureAsync(
            JsonMigrationDataSource source,
            MigrationReadRequest request)
    {
        await using IAsyncEnumerator<MigrationDataBatch> records = source
            .ReadAsync(request, Cancellation)
            .GetAsyncEnumerator(Cancellation);
        Assert.True(await records.MoveNextAsync());
        Assert.Equal(0, records.Current.BatchOrdinal);
        return await Assert.ThrowsAsync<MigrationRowRejectedException>(
            async () => await records.MoveNextAsync());
    }

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
        for (int rowIndex = 0; rowIndex < expected.Rows.Count; rowIndex++)
        {
            MigrationDataRow expectedRow = expected.Rows[rowIndex];
            MigrationDataRow actualRow = actual.Rows[rowIndex];
            Assert.Equal(expectedRow.StableKey, actualRow.StableKey);
            Assert.Equal(expectedRow.Values.Count, actualRow.Values.Count);
            for (int valueIndex = 0;
                 valueIndex < expectedRow.Values.Count;
                 valueIndex++)
            {
                MigrationSourceValue expectedValue =
                    expectedRow.Values[valueIndex];
                MigrationSourceValue actualValue =
                    actualRow.Values[valueIndex];
                Assert.Equal(expectedValue.Kind, actualValue.Kind);
                Assert.Equal(
                    expectedValue.CanonicalText,
                    actualValue.CanonicalText);
                Assert.Equal(
                    expectedValue.BinaryValue.ToArray(),
                    actualValue.BinaryValue.ToArray());
            }
        }

        Assert.Equal(
            expected.RejectedRows.Count,
            actual.RejectedRows.Count);
        for (int rejectIndex = 0;
             rejectIndex < expected.RejectedRows.Count;
             rejectIndex++)
        {
            MigrationRejectedRow expectedRow =
                expected.RejectedRows[rejectIndex];
            MigrationRejectedRow actualRow =
                actual.RejectedRows[rejectIndex];
            Assert.Equal(
                expectedRow.SourceRowOrdinal,
                actualRow.SourceRowOrdinal);
            Assert.Equal(expectedRow.RuleId, actualRow.RuleId);
            Assert.Equal(
                expectedRow.ColumnObjectId,
                actualRow.ColumnObjectId);
            Assert.Equal(
                expectedRow.Evidence.Select(item => item.Name),
                actualRow.Evidence.Select(item => item.Name));
            Assert.Equal(
                expectedRow.Evidence.Select(item => item.Value),
                actualRow.Evidence.Select(item => item.Value));
        }
    }

    private static async Task<List<T>> CollectAsync<T>(
        IAsyncEnumerable<T> values)
    {
        var result = new List<T>();
        await foreach (T value in values.WithCancellation(Cancellation))
            result.Add(value);
        return result;
    }

    private static async Task<List<T>> CollectWithTokenAsync<T>(
        IAsyncEnumerable<T> values,
        CancellationToken cancellationToken)
    {
        var result = new List<T>();
        await foreach (T value in values.WithCancellation(cancellationToken))
            result.Add(value);
        return result;
    }

    private static byte[] Utf8Bytes(string value) =>
        new UTF8Encoding(false, true).GetBytes(value);
}
