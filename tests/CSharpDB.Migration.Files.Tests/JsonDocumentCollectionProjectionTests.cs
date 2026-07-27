using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonDocumentCollectionProjectionTests
{
    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task CatalogFreezesFullKindCountsAndExactCollectionContract()
    {
        const string json =
            """
            [
              null,
              true,
              "text",
              -0,
              {"secretProperty":"secretValue"},
              [1,false]
            ]
            """;
        (JsonSourceSnapshot snapshot, JsonSourceBinding binding) =
            await BoundAsync(
                json,
                JsonInputFraming.RootArray);
        await using (snapshot)
        {
            JsonDocumentCollectionProjectionResult result =
                await JsonDocumentCollectionProjector
                    .ProjectAsync(
                        binding,
                        snapshot,
                        cancellationToken: Cancellation);
            MigrationCatalog catalog = result.CreateCatalog(
                CSharpDbCapabilityCatalogLoader
                    .CurrentTargetVersion);

            Assert.Equal(6, result.TotalRecords);
            Assert.Equal(1, result.NullRecords);
            Assert.Equal(1, result.BooleanRecords);
            Assert.Equal(1, result.StringRecords);
            Assert.Equal(1, result.NumberRecords);
            Assert.Equal(1, result.ObjectRecords);
            Assert.Equal(1, result.ArrayRecords);
            Assert.Equal(
                Encoding.UTF8.GetByteCount(
                    """{"secretProperty":"secretValue"}"""),
                result.MaxCanonicalDocumentBytes);

            Assert.Equal(
                [
                    JsonDocumentCollectionObjectIds
                        .MainNamespace,
                    JsonDocumentCollectionObjectIds.Collection,
                    JsonDocumentCollectionObjectIds.KeyColumn,
                    JsonDocumentCollectionObjectIds
                        .DocumentColumn,
                ],
                catalog.Objects.Select(item => item.ObjectId));
            MigrationCatalogObject collection = Object(
                catalog,
                JsonDocumentCollectionObjectIds.Collection);
            Assert.Equal(
                MigrationObjectKind.Collection,
                collection.Kind);
            Assert.Equal(
                MigrationDocumentCollectionContract
                    .ProjectionContract,
                Facet(
                    collection,
                    MigrationDocumentCollectionContract
                        .ProjectionFacet));
            Assert.Equal(
                MigrationDocumentCollectionContract.RowContract,
                Facet(
                    collection,
                    MigrationDocumentCollectionContract
                        .RowContractFacet));
            Assert.Equal(
                MigrationDocumentCollectionContract.KeyContract,
                Facet(
                    collection,
                    MigrationDocumentCollectionContract
                        .KeyContractFacet));
            Assert.Equal(
                MigrationDocumentCollectionContract
                    .CursorContract,
                Facet(
                    collection,
                    MigrationDocumentCollectionContract
                        .CursorContractFacet));
            Assert.Equal(
                MigrationDocumentCollectionContract
                    .SchemaContract,
                Facet(
                    collection,
                    MigrationDocumentCollectionContract
                        .SchemaContractFacet));
            Assert.Equal(
                MigrationDocumentCollectionContract
                    .DocumentEncoding,
                Facet(
                    collection,
                    MigrationDocumentCollectionContract
                        .DocumentEncodingFacet));
            Assert.Equal(
                "6",
                Facet(collection, "jsonTotalRecords"));
            Assert.Equal(
                "1",
                Facet(collection, "jsonNullRecords"));
            Assert.Equal(
                "1",
                Facet(collection, "jsonBooleanRecords"));
            Assert.Equal(
                "1",
                Facet(collection, "jsonStringRecords"));
            Assert.Equal(
                "1",
                Facet(collection, "jsonNumberRecords"));
            Assert.Equal(
                "1",
                Facet(collection, "jsonObjectRecords"));
            Assert.Equal(
                "1",
                Facet(collection, "jsonArrayRecords"));

            AssertColumn(
                catalog,
                JsonDocumentCollectionObjectIds.KeyColumn,
                MigrationDocumentCollectionContract
                    .KeyColumnName,
                MigrationDocumentCollectionContract.KeyRole,
                MigrationDocumentCollectionContract
                    .TextLogicalType);
            AssertColumn(
                catalog,
                JsonDocumentCollectionObjectIds.DocumentColumn,
                MigrationDocumentCollectionContract
                    .DocumentColumnName,
                MigrationDocumentCollectionContract
                    .DocumentRole,
                MigrationDocumentCollectionContract
                    .JsonLogicalType);

            string artifact =
                MigrationArtifactSerializer.SerializeCatalog(
                    catalog);
            Assert.DoesNotContain(
                "secretProperty",
                artifact,
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                "secretValue",
                artifact,
                StringComparison.Ordinal);

            MigrationPlan plan =
                new MigrationPlanner().CreatePlan(catalog);
            MigrationPlanObject plannedCollection =
                Assert.Single(
                    plan.Objects,
                    item =>
                        item.SourceObjectId ==
                        JsonDocumentCollectionObjectIds
                            .Collection);
            Assert.True(plannedCollection.Included);
            Assert.Null(plannedCollection.ExclusionReason);
            Assert.Equal(
                MigrationResumeMode.TransactionalReceipts,
                plan.Load.ResumeMode);
        }
    }

    [Theory]
    [InlineData(JsonInputFraming.RootArray, "[]")]
    [InlineData(JsonInputFraming.MultipleValues, " \r\n\t")]
    public async Task EmptyInputIsAValidZeroDocumentCollection(
        JsonInputFraming framing,
        string json)
    {
        (JsonSourceSnapshot snapshot, JsonSourceBinding binding) =
            await BoundAsync(json, framing);
        await using (snapshot)
        {
            JsonDocumentCollectionProjectionResult result =
                await JsonDocumentCollectionProjector
                    .ProjectAsync(
                        binding,
                        snapshot,
                        cancellationToken: Cancellation);
            MigrationCatalog catalog = result.CreateCatalog(
                CSharpDbCapabilityCatalogLoader
                    .CurrentTargetVersion);
            await using JsonDocumentCollectionDataSource source =
                await JsonDocumentCollectionDataSource
                    .CreateAsync(
                        result,
                        snapshot,
                        catalog,
                        Cancellation);

            Assert.Equal(0, result.TotalRecords);
            Assert.Equal(0, result.MaxCanonicalDocumentBytes);
            Assert.Empty(await CollectAsync(
                source.ReadAsync(
                    Request(source),
                    Cancellation)));
        }
    }

    [Fact]
    public async Task DuplicateDecodedPropertyNamesAreFatal()
    {
        (JsonSourceSnapshot snapshot, JsonSourceBinding binding) =
            await BoundAsync(
                """[{"a":1,"\u0061":2}]""",
                JsonInputFraming.RootArray);
        await using (snapshot)
        {
            await Assert.ThrowsAsync<JsonReadException>(
                async () => await JsonDocumentCollectionProjector
                    .ProjectAsync(
                        binding,
                        snapshot,
                        cancellationToken: Cancellation));
        }
    }

    [Fact]
    public async Task CollectionNameMustBeABoundedSqlIdentifier()
    {
        (JsonSourceSnapshot snapshot, JsonSourceBinding binding) =
            await BoundAsync(
                "[]",
                JsonInputFraming.RootArray);
        await using (snapshot)
        {
            foreach (string invalid in new[]
                     {
                         "",
                         new string(
                             'x',
                             JsonDocumentCollectionProjectionOptions
                                 .MaximumCollectionNameCharacters +
                             1),
                     })
            {
                await Assert.ThrowsAnyAsync<ArgumentException>(
                    async () =>
                        await JsonDocumentCollectionProjector
                            .ProjectAsync(
                                binding,
                                snapshot,
                                new JsonDocumentCollectionProjectionOptions
                                {
                                    CollectionName = invalid,
                                },
                                Cancellation));
            }

            await Assert.ThrowsAsync<CSharpDbException>(
                async () =>
                    await JsonDocumentCollectionProjector
                        .ProjectAsync(
                            binding,
                            snapshot,
                            new JsonDocumentCollectionProjectionOptions
                            {
                                CollectionName =
                                    "contains\0nul",
                            },
                            Cancellation));
        }
    }

    [Theory]
    [InlineData(
        JsonInputFraming.RootArray,
        """[null,true,"é<\u0001",-0,1e+02,1.2300,{"z":[false,{"a":"é<"}]}]""")]
    [InlineData(
        JsonInputFraming.MultipleValues,
        "null\ntrue\n\"é<\\u0001\"\n-0\n1e+02\n1.2300\n{\"z\":[false,{\"a\":\"é<\"}]}\n")]
    public async Task EveryKindReplaysWithExactCanonicalDocumentsAndKeys(
        JsonInputFraming framing,
        string json)
    {
        await using CollectionFixture fixture =
            await CollectionFixture.CreateAsync(
                json,
                framing);
        List<MigrationDataBatch> batches =
            await CollectAsync(
                fixture.Source.ReadAsync(
                    Request(
                        fixture.Source,
                        batchSize: 20),
                    Cancellation));
        MigrationDataBatch batch = Assert.Single(batches);
        string[] expectedDocuments =
        [
            "null",
            "true",
            "\"é<\\u0001\"",
            "-0",
            "1e+02",
            "1.2300",
            """{"z":[false,{"a":"é<"}]}""",
        ];

        Assert.Equal(
            expectedDocuments,
            batch.Rows.Select(row =>
                row.Values[1].CanonicalText));
        Assert.Equal(
            Enumerable.Range(0, expectedDocuments.Length)
                .Select(index =>
                    MigrationDocumentCollectionContract
                        .FormatOrdinalKey(index)),
            batch.Rows.Select(row => row.StableKey));
        Assert.All(
            batch.Rows,
            row =>
            {
                Assert.Equal(
                    MigrationSourceValueKind.Text,
                    row.Values[0].Kind);
                Assert.Equal(
                    MigrationSourceValueKind.Json,
                    row.Values[1].Kind);
                Assert.Equal(
                    row.StableKey,
                    row.Values[0].CanonicalText);
                Assert.All(
                    row.Values,
                    value => Assert.True(
                        value.BinaryValue.IsEmpty));
            });
    }

    [Fact]
    public async Task RowBridgeRequiresExactKeyThenDocumentProjection()
    {
        await using CollectionFixture fixture =
            await CollectionFixture.CreateAsync(
                """[{"b":2,"a":1}]""",
                JsonInputFraming.RootArray);
        MigrationReadRequest request =
            Request(fixture.Source);
        MigrationDataBatch batch = Assert.Single(
            await CollectAsync(
                fixture.Source.ReadAsync(
                    request,
                    Cancellation)));
        MigrationDataRow row = Assert.Single(batch.Rows);
        Assert.Equal(
            request.ColumnObjectIds,
            batch.ColumnObjectIds);
        Assert.Equal(
            [
                MigrationSourceValueKind.Text,
                MigrationSourceValueKind.Json,
            ],
            row.Values.Select(value => value.Kind));
        Assert.Equal(
            """{"b":2,"a":1}""",
            row.Values[1].CanonicalText);
        Assert.Equal(row.StableKey, row.Values[0].CanonicalText);

        Assert.Throws<ArgumentException>(
            () => fixture.Source.ReadAsync(
                request with
                {
                    ColumnObjectIds =
                    [
                        JsonDocumentCollectionObjectIds
                            .DocumentColumn,
                        JsonDocumentCollectionObjectIds
                            .KeyColumn,
                    ],
                },
                Cancellation));
        Assert.Throws<ArgumentException>(
            () => fixture.Source.ReadAsync(
                request with
                {
                    ColumnObjectIds =
                    [
                        JsonDocumentCollectionObjectIds.KeyColumn,
                    ],
                },
                Cancellation));
    }

    [Fact]
    public async Task ReadBoundsSplitDeterministicallyAndRejectOversizeValues()
    {
        await using CollectionFixture fixture =
            await CollectionFixture.CreateAsync(
                """["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","ccccccccccccccccccccccccccccccccccccccccccccc"]""",
                JsonInputFraming.RootArray);

        List<MigrationDataBatch> batches =
            await CollectAsync(
                fixture.Source.ReadAsync(
                    Request(
                        fixture.Source,
                        batchSize: 20,
                        maxBatchBytes: 100,
                        maxValueBytes: 60),
                    Cancellation));
        Assert.Equal(3, batches.Count);
        Assert.All(
            batches,
            batch => Assert.Single(batch.Rows));

        InvalidDataException valueError =
            await Assert.ThrowsAsync<InvalidDataException>(
                async () => await CollectAsync(
                    fixture.Source.ReadAsync(
                        Request(
                            fixture.Source,
                            maxBatchBytes: 64,
                            maxValueBytes: 40),
                        Cancellation)));
        Assert.Contains(
            JsonDocumentCollectionDataRules
                .ValueSizeExceeded,
            valueError.Message,
            StringComparison.Ordinal);

        InvalidDataException rowError =
            await Assert.ThrowsAsync<InvalidDataException>(
                async () => await CollectAsync(
                    fixture.Source.ReadAsync(
                        Request(
                            fixture.Source,
                            maxBatchBytes: 80,
                            maxValueBytes: 60),
                        Cancellation)));
        Assert.Contains(
            JsonDocumentCollectionDataRules
                .RowSizeExceeded,
            rowError.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task ResumeCursorReplaysExactRemainingBatchBoundary()
    {
        await using CollectionFixture fixture =
            await CollectionFixture.CreateAsync(
                """[{"id":1},{"id":2},{"id":3}]""",
                JsonInputFraming.RootArray);
        MigrationReadRequest request = Request(
            fixture.Source,
            batchSize: 1);
        List<MigrationDataBatch> full =
            await CollectAsync(
                fixture.Source.ReadAsync(
                    request,
                    Cancellation));
        Assert.Equal(3, full.Count);
        string cursor = Assert.IsType<string>(
            full[0].NextCursor);
        Assert.StartsWith(
            MigrationDocumentCollectionContract
                .CursorContract + "/",
            cursor,
            StringComparison.Ordinal);

        List<MigrationDataBatch> resumed =
            await CollectAsync(
                fixture.Source.ReadAsync(
                    request with { ResumeCursor = cursor },
                    Cancellation));
        Assert.Equal(2, resumed.Count);
        AssertBatchEqual(full[1], resumed[0]);
        AssertBatchEqual(full[2], resumed[1]);

        string endCursor = Assert.IsType<string>(
            full[1].NextCursor);
        List<MigrationDataBatch> final =
            await CollectAsync(
                fixture.Source.ReadAsync(
                    request with
                    {
                        ResumeCursor = endCursor,
                    },
                    Cancellation));
        Assert.Single(final);
        AssertBatchEqual(full[2], final[0]);
    }

    [Fact]
    public async Task CursorIsBoundToProjectionBoundsSnapshotAndFailFastMode()
    {
        await using CollectionFixture fixture =
            await CollectionFixture.CreateAsync(
                """[{"private":"TOP-SECRET"},{"private":"next"}]""",
                JsonInputFraming.RootArray,
                logicalIdentity:
                    "C:\\private\\customer-a\\feed.json");
        MigrationReadRequest request = Request(
            fixture.Source,
            batchSize: 1);
        MigrationDataBatch first = (await CollectAsync(
            fixture.Source.ReadAsync(
                request,
                Cancellation)))[0];
        string cursor = Assert.IsType<string>(
            first.NextCursor);

        Assert.DoesNotContain(
            "TOP-SECRET",
            cursor,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "customer-a",
            cursor,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "private",
            cursor,
            StringComparison.Ordinal);

        foreach (MigrationReadRequest drifted in new[]
                 {
                     request with { BatchSize = 2 },
                     request with
                     {
                         MaxBatchBytes = 1024,
                         MaxValueBytes = 1024,
                     },
                     request with { MaxValueBytes = 512 },
                 })
        {
            await Assert.ThrowsAsync<InvalidDataException>(
                async () => await CollectAsync(
                    fixture.Source.ReadAsync(
                        drifted with
                        {
                            ResumeCursor = cursor,
                        },
                        Cancellation)));
        }

        await using CollectionFixture foreign =
            await CollectionFixture.CreateAsync(
                """[{"private":"foreign"},{"private":"next"}]""",
                JsonInputFraming.RootArray);
        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await CollectAsync(
                foreign.Source.ReadAsync(
                    Request(
                        foreign.Source,
                        batchSize: 1) with
                    {
                        ResumeCursor = cursor,
                    },
                    Cancellation)));

        JsonDocumentCollectionProjectionResult renamedProjection =
            await JsonDocumentCollectionProjector.ProjectAsync(
                fixture.Projection.Binding,
                fixture.Snapshot,
                new JsonDocumentCollectionProjectionOptions
                {
                    CollectionName = "renamed_documents",
                },
                Cancellation);
        MigrationCatalog renamedCatalog =
            renamedProjection.CreateCatalog(
                CSharpDbCapabilityCatalogLoader
                    .CurrentTargetVersion);
        await using JsonDocumentCollectionDataSource renamedSource =
            await JsonDocumentCollectionDataSource.CreateAsync(
                renamedProjection,
                fixture.Snapshot,
                renamedCatalog,
                Cancellation);
        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await CollectAsync(
                renamedSource.ReadAsync(
                    Request(
                        renamedSource,
                        batchSize: 1) with
                    {
                        ResumeCursor = cursor,
                    },
                    Cancellation)));

        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await CollectAsync(
                fixture.Source.ReadAsync(
                    request with
                    {
                        ResumeCursor =
                            JsonMigrationDataSource
                                .CursorAlgorithmId +
                            "/1/1/" +
                            new string('0', 64),
                    },
                    Cancellation)));
        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await CollectAsync(
                fixture.Source.ReadAsync(
                    request with
                    {
                        ResumeCursor =
                            cursor[..^1] +
                            (cursor[^1] == '0' ? "1" : "0"),
                    },
                    Cancellation)));
        Assert.Throws<NotSupportedException>(
            () => fixture.Source.ReadAsync(
                request with
                {
                    RejectContractVersion =
                        MigrationRejectContract
                            .DeterministicRejectsV1,
                    RejectPolicy = RejectPolicy(),
                    ResumeCursor = cursor,
                },
                Cancellation));
    }

    [Fact]
    public async Task CreationRejectsSnapshotAndCatalogSubstitution()
    {
        await using CollectionFixture expected =
            await CollectionFixture.CreateAsync(
                """[{"id":1}]""",
                JsonInputFraming.RootArray);
        (JsonSourceSnapshot differentSnapshot, JsonSourceBinding differentBinding) =
            await BoundAsync(
                """[{"id":2}]""",
                JsonInputFraming.RootArray);
        await using (differentSnapshot)
        {
            await Assert.ThrowsAsync<ArgumentException>(
                async () =>
                    await JsonDocumentCollectionDataSource
                        .CreateAsync(
                            expected.Projection,
                            differentSnapshot,
                            expected.Catalog,
                            Cancellation));

            JsonDocumentCollectionProjectionResult different =
                await JsonDocumentCollectionProjector
                    .ProjectAsync(
                        differentBinding,
                        differentSnapshot,
                        cancellationToken: Cancellation);
            MigrationCatalog foreignCatalog =
                different.CreateCatalog(
                    CSharpDbCapabilityCatalogLoader
                        .CurrentTargetVersion);
            await Assert.ThrowsAsync<ArgumentException>(
                async () =>
                    await JsonDocumentCollectionDataSource
                        .CreateAsync(
                            expected.Projection,
                            expected.Snapshot,
                            foreignCatalog,
                            Cancellation));
        }

        MigrationCatalog drifted = expected.Catalog with
        {
            Objects = expected.Catalog.Objects.Select(item =>
                    item.ObjectId ==
                        JsonDocumentCollectionObjectIds.Collection
                        ? item with
                        {
                            SourceName = "different",
                        }
                        : item)
                .ToArray(),
        };
        await Assert.ThrowsAsync<ArgumentException>(
            async () =>
                await JsonDocumentCollectionDataSource
                    .CreateAsync(
                        expected.Projection,
                        expected.Snapshot,
                        drifted,
                        Cancellation));
    }

    private static void AssertColumn(
        MigrationCatalog catalog,
        string objectId,
        string sourceName,
        string role,
        string logicalType)
    {
        MigrationCatalogObject column = Object(
            catalog,
            objectId);
        Assert.Equal(MigrationObjectKind.Column, column.Kind);
        Assert.Equal(sourceName, column.SourceName);
        Assert.Equal(
            JsonDocumentCollectionObjectIds.Collection,
            column.ParentObjectId);
        Assert.Equal(
            role,
            Facet(
                column,
                MigrationDocumentCollectionContract
                    .FieldRoleFacet));
        Assert.Equal(
            logicalType,
            Facet(
                column,
                MigrationDocumentCollectionContract
                    .LogicalTypeFacet));
        Assert.Equal(
            "false",
            Facet(
                column,
                MigrationDocumentCollectionContract
                    .NullableFacet));
    }

    private static MigrationCatalogObject Object(
        MigrationCatalog catalog,
        string objectId) =>
        Assert.Single(
            catalog.Objects,
            item => item.ObjectId == objectId);

    private static string? Facet(
        MigrationCatalogObject item,
        string name) =>
        Assert.Single(
            item.Facets,
            facet => facet.Name == name).Value;

    private static MigrationReadRequest Request(
        JsonDocumentCollectionDataSource source,
        IReadOnlyList<string>? columns = null,
        int batchSize = 100,
        long maxBatchBytes = 64L * 1024 * 1024,
        int maxValueBytes = 16 * 1024 * 1024) =>
        new()
        {
            SourceObjectId =
                JsonDocumentCollectionObjectIds.Collection,
            ColumnObjectIds = columns ??
            [
                JsonDocumentCollectionObjectIds.KeyColumn,
                JsonDocumentCollectionObjectIds.DocumentColumn,
            ],
            BatchSize = batchSize,
            MaxBatchBytes = maxBatchBytes,
            MaxValueBytes = maxValueBytes,
            SnapshotToken = source.SnapshotIdentity,
        };

    private static MigrationDeterministicRejectPolicy
        RejectPolicy() =>
        new()
        {
            ContractVersion =
                MigrationRejectContract.DeterministicRejectsV1,
            AllowedRuleIds =
            [
                JsonMigrationDataRules.NonObjectRow,
            ],
            MaxRejectedRowsPerBatch = 1,
            MaxRejectedRowsPerRun = 1,
            MaxRawValueBytes = 4_096,
            MaxRawValueBytesPerBatch = 4_096,
            MaxRawValueBytesPerRun = 4_096,
            MaxArtifactBytes = 1024 * 1024,
        };

    private static void AssertBatchEqual(
        MigrationDataBatch expected,
        MigrationDataBatch actual)
    {
        Assert.Equal(
            expected.SourceObjectId,
            actual.SourceObjectId);
        Assert.Equal(
            expected.SnapshotIdentity,
            actual.SnapshotIdentity);
        Assert.Equal(
            expected.ColumnObjectIds,
            actual.ColumnObjectIds);
        Assert.Equal(
            expected.BatchOrdinal,
            actual.BatchOrdinal);
        Assert.Equal(
            expected.StartCursor,
            actual.StartCursor);
        Assert.Equal(
            expected.NextCursor,
            actual.NextCursor);
        Assert.Equal(
            expected.Rows.Select(row => row.StableKey),
            actual.Rows.Select(row => row.StableKey));
        Assert.Equal(
            expected.Rows.SelectMany(row => row.Values)
                .Select(value =>
                    (value.Kind, value.CanonicalText)),
            actual.Rows.SelectMany(row => row.Values)
                .Select(value =>
                    (value.Kind, value.CanonicalText)));
    }

    private static async ValueTask<(
        JsonSourceSnapshot Snapshot,
        JsonSourceBinding Binding)> BoundAsync(
        string json,
        JsonInputFraming framing,
        string? logicalIdentity = null)
    {
        JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateAsync(
                new MemoryStream(Utf8(json)),
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
                    logicalIdentity,
                    Cancellation);
            return (snapshot, binding);
        }
        catch
        {
            await snapshot.DisposeAsync();
            throw;
        }
    }

    private static async Task<List<T>> CollectAsync<T>(
        IAsyncEnumerable<T> values)
    {
        var result = new List<T>();
        await foreach (T value in values
                           .WithCancellation(Cancellation))
        {
            result.Add(value);
        }
        return result;
    }

    private static byte[] Utf8(string text) =>
        new UTF8Encoding(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true)
            .GetBytes(text);

    private sealed class CollectionFixture : IAsyncDisposable
    {
        private CollectionFixture(
            JsonSourceSnapshot snapshot,
            JsonDocumentCollectionProjectionResult projection,
            MigrationCatalog catalog,
            JsonDocumentCollectionDataSource source)
        {
            Snapshot = snapshot;
            Projection = projection;
            Catalog = catalog;
            Source = source;
        }

        internal JsonSourceSnapshot Snapshot { get; }

        internal JsonDocumentCollectionProjectionResult Projection { get; }

        internal MigrationCatalog Catalog { get; }

        internal JsonDocumentCollectionDataSource Source { get; }

        internal static async ValueTask<CollectionFixture>
            CreateAsync(
                string json,
                JsonInputFraming framing,
                string? logicalIdentity = null)
        {
            (JsonSourceSnapshot snapshot, JsonSourceBinding binding) =
                await BoundAsync(
                    json,
                    framing,
                    logicalIdentity);
            try
            {
                JsonDocumentCollectionProjectionResult projection =
                    await JsonDocumentCollectionProjector
                        .ProjectAsync(
                            binding,
                            snapshot,
                            cancellationToken:
                                Cancellation);
                MigrationCatalog catalog =
                    projection.CreateCatalog(
                        CSharpDbCapabilityCatalogLoader
                            .CurrentTargetVersion);
                JsonDocumentCollectionDataSource source =
                    await JsonDocumentCollectionDataSource
                        .CreateAsync(
                            projection,
                            snapshot,
                            catalog,
                            Cancellation);
                return new CollectionFixture(
                    snapshot,
                    projection,
                    catalog,
                    source);
            }
            catch
            {
                await snapshot.DisposeAsync();
                throw;
            }
        }

        public async ValueTask DisposeAsync()
        {
            await Source.DisposeAsync();
            await Snapshot.DisposeAsync();
        }
    }
}
