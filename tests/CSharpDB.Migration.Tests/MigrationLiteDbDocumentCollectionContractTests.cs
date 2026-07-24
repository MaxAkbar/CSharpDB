using System.Runtime.CompilerServices;
using System.Text;
using CSharpDB.Engine;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationLiteDbDocumentCollectionContractTests
{
    private const string CollectionId = "litedb:collection:documents";
    private const string KeyColumnId =
        "litedb:collection:documents:key";
    private const string DocumentColumnId =
        "litedb:collection:documents:document";
    private const string StableKey =
        "litedb-key-v1:eyIkYnNvbiI6InN0cmluZyIsInZhbHVlIjoiYWxwaGEifQ";
    private const string TaggedDocument =
        """{"$bson":"document","value":[{"name":"_id","value":{"$bson":"string","value":"alpha"}},{"name":"count","value":{"$bson":"int32","value":1}}]}""";

    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public void SupportedBinder_IdentifiesStableKeysAndAllowsIndexSiblings()
    {
        MigrationCatalog catalog = CreateCatalog(includeIndex: true);
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById =
            catalog.Objects.ToDictionary(
                item => item.ObjectId,
                StringComparer.Ordinal);

        Assert.True(
            MigrationDocumentCollectionContract.TryBindSupportedV1Collection(
                objectsById[CollectionId],
                objectsById,
                out MigrationDocumentCollectionBinding? binding,
                out string? reason),
            reason);
        Assert.Equal(
            MigrationDocumentCollectionKeyMode.StableSourceKey,
            binding!.KeyMode);
        Assert.Equal(KeyColumnId, binding.KeyColumn.ObjectId);
        Assert.Equal(DocumentColumnId, binding.DocumentColumn.ObjectId);

        MigrationPlan plan = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions { AcceptAllExclusions = true });
        Assert.True(PlanObject(plan, CollectionId).Included);
        Assert.True(PlanObject(plan, KeyColumnId).Included);
        MigrationTypeMapping mapping = Assert.Single(
            PlanObject(plan, DocumentColumnId).TypeMappings);
        Assert.Equal(DbType.Text, mapping.TargetType);
        Assert.Equal(
            MigrationMappingClassification.LosslessReencoded,
            mapping.Classification);
        Assert.Equal("canonical-text", mapping.Conversion!.ConversionId);
        Assert.False(PlanObject(plan, $"{CollectionId}:index").Included);
    }

    [Fact]
    public void SupportedBinder_PreservesJsonOrdinalVariant()
    {
        MigrationCatalog catalog = CreateJsonCatalog();
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById =
            catalog.Objects.ToDictionary(
                item => item.ObjectId,
                StringComparer.Ordinal);

        Assert.True(
            MigrationDocumentCollectionContract.TryBindExactV1Collection(
                objectsById["json:collection"],
                objectsById,
                out MigrationCatalogObject? exactKey,
                out MigrationCatalogObject? exactDocument,
                out string? exactReason),
            exactReason);
        Assert.True(
            MigrationDocumentCollectionContract.TryBindSupportedV1Collection(
                objectsById["json:collection"],
                objectsById,
                out MigrationDocumentCollectionBinding? supported,
                out string? supportedReason),
            supportedReason);
        Assert.Equal(
            MigrationDocumentCollectionKeyMode.SourceOrdinal,
            supported!.KeyMode);
        Assert.Equal(exactKey, supported.KeyColumn);
        Assert.Equal(exactDocument, supported.DocumentColumn);
        Assert.Equal(
            "json-ordinal-v1:00000000000000000007",
            MigrationDocumentCollectionContract.FormatOrdinalKey(7));
    }

    [Fact]
    public void TypedKeyValidator_AcceptsCanonicalScalarsAndRejectsMalformedPayloads()
    {
        string[] validPayloads =
        [
            """{"$bson":"min"}""",
            """{"$bson":"int32","value":"1"}""",
            """{"$bson":"int64","value":"-1"}""",
            """{"$bson":"double","value":"3FF0000000000000"}""",
            """{"$bson":"decimal","value":"00000001-00000000-00000000-00000000"}""",
            """{"$bson":"string","value":"alpha"}""",
            """{"$bson":"string","value":"snowman ☃ < & \u000b"}""",
            """{"$bson":"binary","value":"AAE="}""",
            """{"$bson":"objectId","value":"0123456789abcdef01234567"}""",
            """{"$bson":"guid","value":"fc6f2f50-1b7c-4e28-a4a6-dd8f302ef555"}""",
            """{"$bson":"boolean","value":true}""",
            """{"$bson":"dateTime","value":{"ticks":"638500000000000000","kind":"1"}}""",
            """{"$bson":"max"}""",
        ];
        foreach (string payload in validPayloads)
        {
            Assert.True(
                MigrationLiteDbDocumentCollectionContract.TryValidateTypedKey(
                    EncodeTypedKeyPayload(payload),
                    out string? reason),
                reason);
        }

        string[] invalidKeys =
        [
            "wrong-prefix:abc",
            "litedb-key-v1:Zm9yZ2Vk",
            EncodeTypedKeyPayload("""{"$bson":"null"}"""),
            EncodeTypedKeyPayload("""{"$bson":"document","value":[]}"""),
            EncodeTypedKeyPayload("""{ "$bson":"string","value":"alpha"}"""),
            EncodeTypedKeyPayload("""{"value":"alpha","$bson":"string"}"""),
            EncodeTypedKeyPayload("""{"$bson":"int32","value":"01"}"""),
            EncodeTypedKeyPayload("""{"$bson":"double","value":"3ff0000000000000"}"""),
            EncodeTypedKeyPayload(
                """{"$bson":"string","value":"snowman \u2603 \u003C \u0026"}"""),
            EncodeTypedKeyPayload(
                """{"$bson":"dateTime","value":{"kind":"1","ticks":"638500000000000000"}}"""),
            StableKey + "=",
        ];
        foreach (string key in invalidKeys)
        {
            Assert.False(
                MigrationLiteDbDocumentCollectionContract.TryValidateTypedKey(
                    key,
                    out string? reason));
            Assert.NotNull(reason);
        }
    }

    [Fact]
    public void Planner_FailsClosedForUnknownLiteDbContractVariant()
    {
        MigrationCatalog baseline = CreateCatalog();
        MigrationCatalogObject collection = baseline.Objects.Single(
            item => item.ObjectId == CollectionId);
        MigrationCatalog catalog = baseline with
        {
            Objects = baseline.Objects
                .Select(item => item.ObjectId == CollectionId
                    ? collection with
                    {
                        Facets =
                        [
                            .. collection.Facets.Where(facet =>
                                facet.Name !=
                                MigrationLiteDbDocumentCollectionContract
                                    .ProjectionFacet),
                            Facet(
                                MigrationLiteDbDocumentCollectionContract
                                    .ProjectionFacet,
                                "csharpdb-litedb-collection-projection/v2"),
                        ],
                    }
                    : item)
                .ToArray(),
        };
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById =
            catalog.Objects.ToDictionary(
                item => item.ObjectId,
                StringComparer.Ordinal);

        Assert.False(
            MigrationDocumentCollectionContract.TryBindSupportedV1Collection(
                objectsById[CollectionId],
                objectsById,
                out _,
                out string? reason));
        Assert.Contains("supported version 1", reason, StringComparison.Ordinal);

        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        Assert.False(PlanObject(plan, CollectionId).Included);
        Assert.Contains(
            "CSDB-OBJ-COLLECTION-001",
            PlanObject(plan, CollectionId).ExclusionReason,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Target_AppliesStableTypedSourceKeyWithoutOrdinalCoupling()
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        using var files = new TemporaryDirectory();
        await using var source = new CollectionDataSource(
            catalog.Source,
            StableKey,
            StableKey);

        await using (CSharpDbStagedMigrationTarget target =
                     await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            MigrationApplyResult result = await new MigrationApplyRunner()
                .ApplyAsync(
                    new MigrationApplyRequest
                    {
                        Plan = plan,
                        Catalog = catalog,
                        Source = source,
                        Target = target,
                    },
                    Ct);
            Assert.Equal(MigrationApplyStatus.AwaitingValidation, result.Status);
            Assert.Equal(1, result.RowsWritten);
        }

        await using Database database =
            await Database.OpenAsync(files.TargetPath, Ct);
        await using var query = await database.ExecuteAsync(
            "SELECT \"_key\", \"_doc\" FROM \"_col_documents\"",
            Ct);
        Assert.True(await query.MoveNextAsync(Ct));
        Assert.Equal(StableKey, query.Current[0].AsText);
        Assert.Equal(TaggedDocument, query.Current[1].AsText);
        Assert.False(await query.MoveNextAsync(Ct));
    }

    [Theory]
    [InlineData(null, StableKey)]
    [InlineData(StableKey, "litedb-key-v1:Zm9yZ2Vk")]
    public async Task Target_RejectsMissingOrMismatchedStableSourceKey(
        string? stableKey,
        string keyText)
    {
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        using var files = new TemporaryDirectory();
        await using var source = new CollectionDataSource(
            catalog.Source,
            stableKey,
            keyText);
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.CreateNewAsync(
                files.TargetPath,
                plan,
                catalog,
                source.SnapshotIdentity,
                cancellationToken: Ct);

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
            async () => await new MigrationApplyRunner().ApplyAsync(
                new MigrationApplyRequest
                {
                    Plan = plan,
                    Catalog = catalog,
                    Source = source,
                    Target = target,
                },
                Ct));
        Assert.Contains(
            "bound stable key",
            error.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Target_RejectsMalformedButEqualStableSourceKey()
    {
        const string malformedKey = "litedb-key-v1:Zm9yZ2Vk";
        MigrationCatalog catalog = CreateCatalog();
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        using var files = new TemporaryDirectory();
        await using var source = new CollectionDataSource(
            catalog.Source,
            malformedKey,
            malformedKey);
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.CreateNewAsync(
                files.TargetPath,
                plan,
                catalog,
                source.SnapshotIdentity,
                cancellationToken: Ct);

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
            async () => await new MigrationApplyRunner().ApplyAsync(
                new MigrationApplyRequest
                {
                    Plan = plan,
                    Catalog = catalog,
                    Source = source,
                    Target = target,
                },
                Ct));
        Assert.Contains(
            "valid version 1 typed stable key",
            error.Message,
            StringComparison.Ordinal);
    }

    private static MigrationCatalog CreateCatalog(
        bool includeIndex = false) => new()
        {
            TargetCSharpDbVersion =
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            Source = Source(MigrationSourceKind.LiteDb, "litedb:test"),
            Objects =
            [
                new MigrationCatalogObject
                {
                    ObjectId = CollectionId,
                    Kind = MigrationObjectKind.Collection,
                    SourceName = "documents",
                    Facets = MigrationLiteDbDocumentCollectionContract
                        .RequiredCollectionFacets,
                },
                new MigrationCatalogObject
                {
                    ObjectId = KeyColumnId,
                    Kind = MigrationObjectKind.Column,
                    ParentObjectId = CollectionId,
                    SourceName =
                        MigrationLiteDbDocumentCollectionContract.KeyColumnName,
                    NativeType =
                        MigrationLiteDbDocumentCollectionContract.KeyNativeType,
                    Facets = MigrationLiteDbDocumentCollectionContract
                        .CreateKeyFacets(),
                },
                new MigrationCatalogObject
                {
                    ObjectId = DocumentColumnId,
                    Kind = MigrationObjectKind.Column,
                    ParentObjectId = CollectionId,
                    SourceName =
                        MigrationLiteDbDocumentCollectionContract
                            .DocumentColumnName,
                    NativeType =
                        MigrationLiteDbDocumentCollectionContract
                            .DocumentNativeType,
                    Facets = MigrationLiteDbDocumentCollectionContract
                        .CreateDocumentFacets(),
                },
                .. includeIndex
                    ? new[]
                    {
                        new MigrationCatalogObject
                        {
                            ObjectId = $"{CollectionId}:index",
                            Kind = MigrationObjectKind.Index,
                            ParentObjectId = CollectionId,
                            SourceName = "unsupported_expression_index",
                        },
                    }
                    : [],
            ],
        };

    private static MigrationCatalog CreateJsonCatalog() => new()
    {
        TargetCSharpDbVersion =
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
        Source = Source(MigrationSourceKind.Json, "json:test"),
        Objects =
        [
            new MigrationCatalogObject
            {
                ObjectId = "json:collection",
                Kind = MigrationObjectKind.Collection,
                SourceName = "documents",
                Facets =
                [
                    Facet(
                        MigrationDocumentCollectionContract.ProjectionFacet,
                        MigrationDocumentCollectionContract.ProjectionContract),
                    Facet(
                        MigrationDocumentCollectionContract.RowContractFacet,
                        MigrationDocumentCollectionContract.RowContract),
                    Facet(
                        MigrationDocumentCollectionContract.KeyContractFacet,
                        MigrationDocumentCollectionContract.KeyContract),
                    Facet(
                        MigrationDocumentCollectionContract.CursorContractFacet,
                        MigrationDocumentCollectionContract.CursorContract),
                    Facet(
                        MigrationDocumentCollectionContract.SchemaContractFacet,
                        MigrationDocumentCollectionContract.SchemaContract),
                    Facet(
                        MigrationDocumentCollectionContract
                            .DocumentEncodingFacet,
                        MigrationDocumentCollectionContract.DocumentEncoding),
                ],
            },
            JsonColumn(
                "json:collection:key",
                MigrationDocumentCollectionContract.KeyColumnName,
                MigrationDocumentCollectionContract.KeyNativeType,
                MigrationDocumentCollectionContract.TextLogicalType,
                MigrationDocumentCollectionContract.KeyRole),
            JsonColumn(
                "json:collection:document",
                MigrationDocumentCollectionContract.DocumentColumnName,
                MigrationDocumentCollectionContract.DocumentNativeType,
                MigrationDocumentCollectionContract.JsonLogicalType,
                MigrationDocumentCollectionContract.DocumentRole),
        ],
    };

    private static MigrationCatalogObject JsonColumn(
        string objectId,
        string name,
        string nativeType,
        string logicalType,
        string role) => new()
        {
            ObjectId = objectId,
            Kind = MigrationObjectKind.Column,
            ParentObjectId = "json:collection",
            SourceName = name,
            NativeType = nativeType,
            Facets =
            [
                Facet(
                    MigrationDocumentCollectionContract.LogicalTypeFacet,
                    logicalType),
                Facet(
                    MigrationDocumentCollectionContract.NullableFacet,
                    "false"),
                Facet(
                    MigrationDocumentCollectionContract.FieldRoleFacet,
                    role),
                Facet(
                    role == MigrationDocumentCollectionContract.KeyRole
                        ? MigrationDocumentCollectionContract.KeyContractFacet
                        : MigrationDocumentCollectionContract
                            .DocumentEncodingFacet,
                    role == MigrationDocumentCollectionContract.KeyRole
                        ? MigrationDocumentCollectionContract.KeyContract
                        : MigrationDocumentCollectionContract.DocumentEncoding),
            ],
        };

    private static MigrationSourceIdentity Source(
        MigrationSourceKind kind,
        string identity) => new()
        {
            Kind = kind,
            Identity = identity,
            Fingerprint =
                "sha256:4b2ca370f7ed96273237437592773679222eac1356c9b7050fdfa51da98ea17d",
            ProviderVersion = "test-v1",
            SourceVersion = "fixture-v1",
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Immutable,
                Description = "Immutable document collection test fixture.",
            },
        };

    private static MigrationPlanObject PlanObject(
        MigrationPlan plan,
        string objectId) =>
        plan.Objects.Single(item => item.SourceObjectId == objectId);

    private static MigrationCatalogFacet Facet(
        string name,
        string value) => new()
        {
            Name = name,
            Value = value,
        };

    private static string EncodeTypedKeyPayload(string payload) =>
        MigrationLiteDbDocumentCollectionContract.TypedKeyPrefix +
        Convert.ToBase64String(Encoding.UTF8.GetBytes(payload))
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');

    private sealed class CollectionDataSource(
        MigrationSourceIdentity source,
        string? stableKey,
        string keyText) : IMigrationDataSource
    {
        public MigrationSourceIdentity Source { get; } = source;

        public string SnapshotIdentity { get; } =
            "litedb-snapshot:test-v1";

        public async IAsyncEnumerable<MigrationDataBatch> ReadAsync(
            MigrationReadRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken =
                default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (request.SourceObjectId != CollectionId)
                throw new InvalidDataException("Unexpected collection.");

            yield return new MigrationDataBatch
            {
                SourceObjectId = CollectionId,
                SnapshotIdentity = SnapshotIdentity,
                ColumnObjectIds = request.ColumnObjectIds.ToArray(),
                Rows =
                [
                    new MigrationDataRow
                    {
                        StableKey = stableKey,
                        Values = request.ColumnObjectIds
                            .Select(columnId => new MigrationSourceValue
                            {
                                Kind = MigrationSourceValueKind.Text,
                                CanonicalText = columnId == KeyColumnId
                                    ? keyText
                                    : TaggedDocument,
                            })
                            .ToArray(),
                    },
                ],
            };
            await Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class TemporaryDirectory : IDisposable
    {
        internal TemporaryDirectory()
        {
            DirectoryPath = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-litedb-contract-{Guid.NewGuid():N}");
            Directory.CreateDirectory(DirectoryPath);
            TargetPath = Path.Combine(DirectoryPath, "target.csdb");
        }

        private string DirectoryPath { get; }

        internal string TargetPath { get; }

        public void Dispose()
        {
            try
            {
                Directory.Delete(DirectoryPath, recursive: true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }
}
