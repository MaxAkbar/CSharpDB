using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Migration.LiteDb;
using LiteDB;

namespace CSharpDB.Migration.LiteDb.Tests;

public sealed class LiteDbCanonicalBsonCodecTests
{
    [Fact]
    public void EncodeDocumentPreservesEveryBsonTypeAndOrdersPropertiesOrdinally()
    {
        double nonFinite = BitConverter.Int64BitsToDouble(
            unchecked((long)0xFFF8000000000042UL));
        decimal exactDecimal = new(
            lo: 123456789,
            mid: 987654321,
            hi: 12345,
            isNegative: true,
            scale: 17);
        var document = new BsonDocument
        {
            ["z"] = BsonValue.MaxValue,
            ["string"] = "snowman \u2603 < & \u2028 \uE000 \u000B",
            ["null"] = BsonValue.Null,
            ["int32"] = 42,
            ["int64"] = 42L,
            ["double"] = nonFinite,
            ["decimal"] = exactDecimal,
            ["document"] = new BsonDocument { ["b"] = 2, ["a"] = 1 },
            ["array"] = new BsonArray { 1, "two", BsonValue.Null },
            ["binary"] = new byte[] { 0, 1, 254, 255 },
            ["objectId"] = ObjectId.NewObjectId(),
            ["guid"] = Guid.Parse("fc6f2f50-1b7c-4e28-a4a6-dd8f302ef555"),
            ["boolean"] = true,
            ["dateTime"] = new DateTime(
                638500000000000000,
                DateTimeKind.Utc),
            ["min"] = BsonValue.MinValue,
        };

        string first = LiteDbCanonicalBsonCodec.EncodeDocument(document);
        string second = LiteDbCanonicalBsonCodec.EncodeDocument(document);

        Assert.Equal(first, second);
        foreach (string type in new[]
                 {
                     "min", "null", "int32", "int64", "double", "decimal",
                     "string", "document", "array", "binary", "objectId",
                     "guid", "boolean", "dateTime", "max",
                 })
        {
            Assert.Contains($"\"$bson\":\"{type}\"", first, StringComparison.Ordinal);
        }
        Assert.Contains(
            unchecked((ulong)BitConverter.DoubleToInt64Bits(nonFinite))
                .ToString("X16", CultureInfo.InvariantCulture),
            first,
            StringComparison.Ordinal);
        Assert.Contains(DecimalBits(exactDecimal), first, StringComparison.Ordinal);
        Assert.Contains(
            "\"ticks\":\"638500000000000000\"",
            first,
            StringComparison.Ordinal);
        Assert.Contains("\"kind\":\"1\"", first, StringComparison.Ordinal);
        Assert.Contains(
            "snowman \u2603 < & \u2028 \uE000 \\u000b",
            first,
            StringComparison.Ordinal);
        Assert.DoesNotContain("\\u2603", first, StringComparison.Ordinal);
        Assert.DoesNotContain("\\u003c", first, StringComparison.OrdinalIgnoreCase);
        Assert.True(
            first.IndexOf("\"name\":\"a\"", StringComparison.Ordinal) <
            first.IndexOf("\"name\":\"b\"", StringComparison.Ordinal));
    }

    [Fact]
    public void EncodeDocumentIsIndependentOfInsertionOrderAndMarkerNamesCannotCollide()
    {
        var first = new BsonDocument
        {
            ["value"] = new BsonDocument { ["$bson"] = "int32" },
            ["$bson"] = "document",
            ["name"] = "value",
        };
        var second = new BsonDocument
        {
            ["name"] = "value",
            ["$bson"] = "document",
            ["value"] = new BsonDocument { ["$bson"] = "int32" },
        };

        string encoded = LiteDbCanonicalBsonCodec.EncodeDocument(first);

        Assert.Equal(encoded, LiteDbCanonicalBsonCodec.EncodeDocument(second));
        Assert.Contains("\"name\":\"$bson\"", encoded, StringComparison.Ordinal);
        Assert.Contains(
            "\"value\":{\"$bson\":\"string\",\"value\":\"document\"}",
            encoded,
            StringComparison.Ordinal);
        Assert.NotEqual(
            encoded,
            LiteDbCanonicalBsonCodec.EncodeDocument(
                new BsonDocument { ["value"] = 1 }));
    }

    [Fact]
    public void TypedKeyIsRepeatableAndDistinguishesBsonTypes()
    {
        string int32 = LiteDbCanonicalBsonCodec.EncodeTypedKey(new BsonValue(1));
        string int64 = LiteDbCanonicalBsonCodec.EncodeTypedKey(new BsonValue(1L));
        string text = LiteDbCanonicalBsonCodec.EncodeTypedKey(new BsonValue("1"));
        string richText = LiteDbCanonicalBsonCodec.EncodeTypedKey(
            new BsonValue("snowman \u2603 < & \u000B"));

        Assert.StartsWith(LiteDbCanonicalBsonCodec.TypedKeyPrefix, int32);
        Assert.Equal(
            int32,
            LiteDbCanonicalBsonCodec.EncodeTypedKey(new BsonValue(1)));
        Assert.Equal(4, new HashSet<string> { int32, int64, text, richText }.Count);
        foreach (string key in new[] { int32, int64, text, richText })
        {
            Assert.True(
                MigrationLiteDbDocumentCollectionContract.TryValidateTypedKey(
                    key,
                    out string? reason),
                reason);
        }
        Assert.Throws<LiteDbMigrationException>(
            () => LiteDbCanonicalBsonCodec.EncodeTypedKey(BsonValue.Null));
        Assert.Throws<LiteDbMigrationException>(
            () => LiteDbCanonicalBsonCodec.EncodeTypedKey(
                new BsonDocument { ["nested"] = 1 }));
    }

    [Fact]
    public void CodecRejectsInvalidUnicodeAndEveryConfiguredBound()
    {
        string invalid = new('\uD800', 1);
        var strict = new LiteDbInspectionLimits
        {
            MaxDepth = 1,
            MaxFieldsPerDocument = 2,
            MaxPropertyNameBytes = 4,
            MaxStringBytes = 4,
            MaxBinaryBytes = 3,
            MaxCanonicalOutputBytes = 128,
            MaxTypedKeyBytes = 40,
            MaxPathBytes = 8,
        };

        Assert.Throws<LiteDbMigrationException>(
            () => LiteDbCanonicalBsonCodec.EncodeDocument(
                new BsonDocument { ["bad"] = invalid }));
        Assert.Throws<LiteDbMigrationException>(
            () => LiteDbCanonicalBsonCodec.EncodeDocument(
                new BsonDocument { [invalid] = 1 }));
        Assert.Throws<LiteDbMigrationException>(
            () => LiteDbCanonicalBsonCodec.EncodeDocument(
                new BsonDocument
                {
                    ["a"] = new BsonDocument
                    {
                        ["b"] = new BsonDocument { ["c"] = 1 },
                    },
                },
                strict));
        Assert.Throws<LiteDbMigrationException>(
            () => LiteDbCanonicalBsonCodec.EncodeDocument(
                new BsonDocument { ["a"] = 1, ["b"] = 2, ["c"] = 3 },
                strict));
        Assert.Throws<LiteDbMigrationException>(
            () => LiteDbCanonicalBsonCodec.EncodeDocument(
                new BsonDocument { ["long-name"] = 1 },
                strict));
        Assert.Throws<LiteDbMigrationException>(
            () => LiteDbCanonicalBsonCodec.EncodeDocument(
                new BsonDocument { ["a"] = "12345" },
                strict));
        Assert.Throws<LiteDbMigrationException>(
            () => LiteDbCanonicalBsonCodec.EncodeDocument(
                new BsonDocument { ["a"] = new byte[] { 1, 2, 3, 4 } },
                strict));
        Assert.Throws<LiteDbMigrationException>(
            () => LiteDbCanonicalBsonCodec.EncodeTypedKey(
                new BsonValue("a-key-that-is-too-long"),
                strict));
        Assert.Throws<LiteDbMigrationException>(
            () => LiteDbCanonicalBsonCodec.EncodeDocument(
                new BsonDocument { ["a"] = new string('x', 40) },
                strict with
                {
                    MaxStringBytes = 100,
                    MaxCanonicalOutputBytes = 64,
                    MaxPropertyNameBytes = 100,
                    MaxPathBytes = 100,
                }));
        Assert.Throws<LiteDbMigrationException>(
            () => LiteDbCanonicalBsonCodec.EncodeDocument(
                new BsonDocument { ["a"] = 1 },
                strict with
                {
                    MaxDepth = 64,
                    MaxFieldsPerDocument = 100,
                    MaxPropertyNameBytes = 100,
                    MaxPathBytes = 4,
                }));
        Assert.Throws<LiteDbMigrationException>(
            () => LiteDbCanonicalBsonCodec.EncodeDocument(
                new BsonDocument { ["a"] = 1 },
                strict with
                {
                    MaxDepth = 64,
                    MaxFieldsPerDocument = 100,
                    MaxPropertyNameBytes = 100,
                    MaxPathBytes = 100,
                    MaxJsonNodes = 7,
                    MaxJsonContainerDepth = 128,
                }));
        Assert.Throws<LiteDbMigrationException>(
            () => LiteDbCanonicalBsonCodec.EncodeDocument(
                new BsonDocument { ["a"] = 1 },
                strict with
                {
                    MaxDepth = 64,
                    MaxFieldsPerDocument = 100,
                    MaxPropertyNameBytes = 100,
                    MaxPathBytes = 100,
                    MaxJsonNodes = 100,
                    MaxJsonContainerDepth = 3,
                }));
        _ = LiteDbCanonicalBsonCodec.EncodeDocument(
            new BsonDocument { ["a"] = 1 },
            strict with
            {
                MaxDepth = 64,
                MaxFieldsPerDocument = 100,
                MaxPropertyNameBytes = 100,
                MaxPathBytes = 100,
                MaxJsonNodes = 8,
                MaxJsonContainerDepth = 4,
            });
    }

    [Fact]
    public async Task CodecProducedKeyAndDocumentPassTheRealStagedCollectionTarget()
    {
        const string collectionId = "litedb:test:collection";
        const string keyColumnId = "litedb:test:key";
        const string documentColumnId = "litedb:test:document";
        const string richText = "snowman \u2603 < & \u000B";
        string typedKey = LiteDbCanonicalBsonCodec.EncodeTypedKey(
            new BsonValue(richText));
        string encodedDocument = LiteDbCanonicalBsonCodec.EncodeDocument(
            new BsonDocument
            {
                ["_id"] = richText,
                ["text"] = richText,
            });
        MigrationCatalog catalog = InteropCatalog(
            collectionId,
            keyColumnId,
            documentColumnId);
        MigrationPlan plan = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions { AcceptAllExclusions = true });
        Assert.True(plan.Objects.Single(item =>
            item.SourceObjectId == collectionId).Included);

        string directory = Path.Combine(
            Path.GetTempPath(),
            "csharpdb-litedb-codec-target",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        string targetPath = Path.Combine(directory, "target.csdb");
        try
        {
            await using var source = new SingleDocumentSource(
                catalog,
                collectionId,
                keyColumnId,
                documentColumnId,
                typedKey,
                encodedDocument);
            await using (CSharpDbStagedMigrationTarget target =
                         await CSharpDbStagedMigrationTarget.CreateNewAsync(
                             targetPath,
                             plan,
                             catalog,
                             source.SnapshotIdentity,
                             cancellationToken: TestContext.Current.CancellationToken))
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
                        TestContext.Current.CancellationToken);
                Assert.Equal(MigrationApplyStatus.AwaitingValidation, result.Status);
                Assert.Equal(1, result.RowsWritten);
            }

            await using Database database = await Database.OpenAsync(
                targetPath,
                TestContext.Current.CancellationToken);
            Collection<JsonElement> collection =
                await database.GetCollectionAsync<JsonElement>(
                    "interop",
                    TestContext.Current.CancellationToken);
            JsonElement? stored = await collection.GetAsync(
                typedKey,
                TestContext.Current.CancellationToken);
            Assert.True(stored.HasValue);
            Assert.Equal(encodedDocument, stored.Value.GetRawText());
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    private static string DecimalBits(decimal value) =>
        string.Join(
            "-",
            decimal.GetBits(value)
                .Select(static part =>
                    unchecked((uint)part).ToString("X8", CultureInfo.InvariantCulture)));

    private static MigrationCatalog InteropCatalog(
        string collectionId,
        string keyColumnId,
        string documentColumnId)
    {
        const string namespaceId = "litedb:test:namespace";
        MigrationCatalog catalog = new()
        {
            TargetCSharpDbVersion =
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            Source = new MigrationSourceIdentity
            {
                Kind = MigrationSourceKind.LiteDb,
                Identity = "litedb-test:codec-target",
                Fingerprint = "sha256:" + new string('a', 64),
                ProviderVersion = "5.0.21",
                SourceVersion = "5",
                Consistency = new MigrationConsistencyStrategy
                {
                    Kind = MigrationConsistencyKind.Immutable,
                    Description = "In-memory codec interoperability fixture.",
                },
            },
            Objects =
            [
                new MigrationCatalogObject
                {
                    ObjectId = namespaceId,
                    Kind = MigrationObjectKind.Namespace,
                    SourceName = "main",
                    Facets =
                    [
                        new MigrationCatalogFacet
                        {
                            Name = "isDefault",
                            Value = "true",
                        },
                    ],
                },
                new MigrationCatalogObject
                {
                    ObjectId = collectionId,
                    Kind = MigrationObjectKind.Collection,
                    ParentObjectId = namespaceId,
                    SourceNamespace = "main",
                    SourceName = "interop",
                    Facets =
                        MigrationLiteDbDocumentCollectionContract
                            .RequiredCollectionFacets,
                },
                new MigrationCatalogObject
                {
                    ObjectId = keyColumnId,
                    Kind = MigrationObjectKind.Column,
                    ParentObjectId = collectionId,
                    SourceNamespace = "main",
                    SourceName =
                        MigrationLiteDbDocumentCollectionContract.KeyColumnName,
                    NativeType =
                        MigrationLiteDbDocumentCollectionContract.KeyNativeType,
                    Facets =
                        MigrationLiteDbDocumentCollectionContract.CreateKeyFacets(),
                },
                new MigrationCatalogObject
                {
                    ObjectId = documentColumnId,
                    Kind = MigrationObjectKind.Column,
                    ParentObjectId = collectionId,
                    SourceNamespace = "main",
                    SourceName =
                        MigrationLiteDbDocumentCollectionContract.DocumentColumnName,
                    NativeType =
                        MigrationLiteDbDocumentCollectionContract.DocumentNativeType,
                    Facets =
                        MigrationLiteDbDocumentCollectionContract.CreateDocumentFacets(),
                },
            ],
        };
        MigrationContractValidator.ValidateCatalog(catalog);
        return catalog;
    }

    private sealed class SingleDocumentSource(
        MigrationCatalog catalog,
        string collectionId,
        string keyColumnId,
        string documentColumnId,
        string typedKey,
        string encodedDocument) :
        IMigrationDataSource,
        IMigrationCatalogBoundDataSource
    {
        public MigrationSourceIdentity Source => catalog.Source;

        public string SnapshotIdentity => "litedb-test-snapshot";

        public string CatalogDigest =>
            MigrationArtifactSerializer.ComputeCatalogDigest(catalog);

        public async IAsyncEnumerable<MigrationDataBatch> ReadAsync(
            MigrationReadRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Assert.Equal(collectionId, request.SourceObjectId);
            MigrationSourceValue[] values = request.ColumnObjectIds
                .Select(columnId =>
                    columnId == keyColumnId
                        ? new MigrationSourceValue
                        {
                            Kind = MigrationSourceValueKind.Text,
                            CanonicalText = typedKey,
                        }
                        : columnId == documentColumnId
                            ? new MigrationSourceValue
                            {
                                Kind = MigrationSourceValueKind.Json,
                                CanonicalText = encodedDocument,
                            }
                            : throw new InvalidOperationException(
                                "The target requested an unknown interop column."))
                .ToArray();
            yield return new MigrationDataBatch
            {
                SourceObjectId = collectionId,
                SnapshotIdentity = SnapshotIdentity,
                ColumnObjectIds = request.ColumnObjectIds,
                BatchOrdinal = 0,
                Rows =
                [
                    new MigrationDataRow
                    {
                        StableKey = typedKey,
                        Values = values,
                    },
                ],
            };
            await Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
