using System.Security.Cryptography;
using CSharpDB.Migration;
using CSharpDB.Migration.LiteDb;
using LiteDB;

namespace CSharpDB.Migration.LiteDb.Tests;

public sealed class LiteDbMigrationSourceInspectorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task InspectProducesDeterministicValidCatalogWithoutMutatingSource()
    {
        using var fixture = LiteDbFixture.Create();
        SourceState before = SourceState.Read(fixture.Path);
        var inspector = new LiteDbMigrationSourceInspector(fixture.Path);

        MigrationCatalog first = await InspectAsync(inspector, includeProfile: true);
        MigrationCatalog second = await InspectAsync(inspector, includeProfile: true);
        SourceState after = SourceState.Read(fixture.Path);

        Assert.Equal(before.Sha256, after.Sha256);
        Assert.Equal(before.Length, after.Length);
        Assert.Equal(before.LastWriteTicks, after.LastWriteTicks);
        Assert.Equal(before.Files, after.Files);
        Assert.Equal(
            MigrationArtifactSerializer.SerializeCatalog(first),
            MigrationArtifactSerializer.SerializeCatalog(second));
        Assert.Equal(
            MigrationArtifactSerializer.ComputeCatalogDigest(first),
            MigrationArtifactSerializer.ComputeCatalogDigest(second));
        MigrationContractValidator.ValidateCatalog(first);
        Assert.Equal(MigrationSourceKind.LiteDb, first.Source.Kind);
        Assert.Equal("sha256:" + before.Sha256, first.Source.Fingerprint);
        Assert.Equal(MigrationConsistencyKind.BestEffort, first.Source.Consistency.Kind);
        Assert.DoesNotContain(fixture.Path, first.Source.Identity, StringComparison.OrdinalIgnoreCase);

        string serialized = MigrationArtifactSerializer.SerializeCatalog(first);
        Assert.DoesNotContain(
            LiteDbFixture.SecretValue,
            serialized,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task InspectInventoriesMultipleEmptyAndUnusualCollectionsAndRecordsCaseFolding()
    {
        using var fixture = LiteDbFixture.Create();

        MigrationCatalog catalog = await InspectAsync(
            new LiteDbMigrationSourceInspector(fixture.Path),
            includeProfile: true);
        MigrationCatalogObject[] collections = catalog.Objects
            .Where(static item => item.Kind == MigrationObjectKind.Collection)
            .OrderBy(static item => item.SourceName, StringComparer.Ordinal)
            .ToArray();

        Assert.Contains(collections, item => item.SourceName == "People");
        Assert.DoesNotContain(collections, item => item.SourceName == "people");
        Assert.Contains(collections, item => item.SourceName == "_empty_01");
        Assert.Contains(collections, item => item.SourceName == "_odd_01");
        Assert.Equal(
            collections.Length,
            collections.Select(static item => item.ObjectId).Distinct(StringComparer.Ordinal).Count());
        MigrationCatalogObject main = catalog.Objects.Single(
            static item => item.Kind == MigrationObjectKind.Namespace);
        Assert.Equal(
            "ordinal-ignore-case",
            Facet(main, "liteDbCollectionNameComparison"));
        Assert.DoesNotContain(
            catalog.Diagnostics,
            static item =>
                item.RuleId == "MIG-LITEDB-COLLECTION-NAME-COLLISION-001");

        foreach (MigrationCatalogObject collection in collections)
        {
            IReadOnlyDictionary<string, MigrationCatalogObject> byId = catalog.Objects
                .ToDictionary(static item => item.ObjectId, StringComparer.Ordinal);
            Assert.True(
                MigrationDocumentCollectionContract.TryBindSupportedV1Collection(
                    collection,
                    byId,
                    out MigrationDocumentCollectionBinding? binding,
                    out string? reason),
                reason);
            Assert.Equal(MigrationDocumentCollectionKeyMode.StableSourceKey, binding!.KeyMode);
        }

        MigrationCatalogObject empty = collections.Single(
            static item => item.SourceName == "_empty_01");
        Assert.Equal("0", Facet(empty, "liteDbDocumentCount"));
        Assert.Equal("full", Facet(empty, "liteDbProfileCoverage"));
    }

    [Fact]
    public async Task FullProfileReportsPresenceAndBsonTypesWithoutScalarValues()
    {
        using var fixture = LiteDbFixture.Create();

        MigrationCatalog catalog = await InspectAsync(
            new LiteDbMigrationSourceInspector(fixture.Path),
            includeProfile: true);
        MigrationCatalogObject people = catalog.Objects.Single(
            static item =>
                item.Kind == MigrationObjectKind.Collection &&
                item.SourceName == "People");
        string[] profiles = people.Facets
            .Where(static facet =>
                facet.Name.StartsWith("liteDbProfileField", StringComparison.Ordinal) &&
                facet.Name != "liteDbProfileFieldPathCount")
            .Select(static facet => facet.Value!)
            .ToArray();

        Assert.NotEmpty(profiles);
        Assert.Contains(profiles, value =>
            value.Contains("types=int32:1,string:1", StringComparison.Ordinal));
        Assert.Contains(profiles, value =>
            value.Contains("document:", StringComparison.Ordinal));
        Assert.Contains(profiles, value =>
            value.Contains("array:", StringComparison.Ordinal));
        Assert.Equal("full", Facet(people, "liteDbProfileCoverage"));
        Assert.Equal("3", Facet(people, "liteDbProfileValuesExamined"));
        Assert.Contains("int32:2", Facet(people, "liteDbIdTypeCounts"), StringComparison.Ordinal);
        Assert.Contains("string:1", Facet(people, "liteDbIdTypeCounts"), StringComparison.Ordinal);
        Assert.DoesNotContain(
            LiteDbFixture.SecretValue,
            string.Join("\n", profiles),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task IndexInventoryCoversIdSimpleUniqueExpressionAndCollationDiagnostics()
    {
        using var fixture = LiteDbFixture.Create();
        int nativeIndexCount;
        using (var database = LiteDbFixture.OpenReadOnly(fixture.Path))
        {
            nativeIndexCount = database
                .GetCollection("$indexes", BsonAutoId.Int32)
                .FindAll()
                .Count();
        }

        MigrationCatalog catalog = await InspectAsync(
            new LiteDbMigrationSourceInspector(fixture.Path),
            includeProfile: false);
        MigrationCatalogObject[] indexes = catalog.Objects
            .Where(static item => item.Kind == MigrationObjectKind.Index)
            .ToArray();
        string[] rules = catalog.Diagnostics
            .Select(static item => item.RuleId)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(nativeIndexCount, indexes.Length);
        Assert.Contains(indexes, item => item.SourceName == "_id");
        Assert.Contains(indexes, item => item.SourceName == "ix_name");
        Assert.Contains(indexes, item => item.SourceName == "ux_code");
        Assert.Contains(indexes, item => item.SourceName == "ix_name_lower");
        Assert.Contains("MIG-LITEDB-INDEX-ID-001", rules);
        Assert.Contains("MIG-LITEDB-INDEX-SIMPLE-001", rules);
        Assert.Contains("MIG-LITEDB-INDEX-UNIQUE-001", rules);
        Assert.Contains("MIG-LITEDB-INDEX-EXPRESSION-001", rules);
        Assert.Contains("MIG-LITEDB-INDEX-COLLATION-001", rules);
        Assert.All(indexes, static index =>
        {
            Assert.NotNull(Facet(index, "liteDbIndexExpressionDigest"));
            Assert.NotNull(Facet(index, "liteDbIndexExpression"));
            Assert.NotNull(Facet(index, "liteDbIndexShape"));
        });

        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        MigrationCatalogObject people = catalog.Objects.Single(
            static item =>
                item.Kind == MigrationObjectKind.Collection &&
                item.SourceName == "People");
        MigrationCatalogObject idIndex = indexes.Single(item =>
            item.ParentObjectId == people.ObjectId &&
            item.SourceName == "_id");
        MigrationPlanObject plannedId = plan.Objects.Single(
            item => item.SourceObjectId == idIndex.ObjectId);
        Assert.False(plannedId.Included);
        Assert.Empty(idIndex.DependsOn);
        Assert.Empty(idIndex.Members);
        Assert.Equal("true", Facet(idIndex, "unique"));
        Assert.Equal("standard", Facet(idIndex, "kind"));
        Assert.Contains(
            catalog.Diagnostics,
            item =>
                item.ObjectId == idIndex.ObjectId &&
                item.RuleId == "MIG-LITEDB-INDEX-ID-001" &&
                item.Status == MigrationCompatibilityStatus.CompatibleWithRewrite);
    }

    [Fact]
    public async Task InspectionBoundsAndCancellationFailClosed()
    {
        using var fixture = LiteDbFixture.Create();
        var bounded = new LiteDbMigrationSourceInspector(
            fixture.Path,
            password: null,
            new LiteDbInspectionLimits { MaxDocuments = 3 });

        LiteDbMigrationException error = await Assert.ThrowsAsync<LiteDbMigrationException>(
            async () => await InspectAsync(bounded, includeProfile: true));
        Assert.Contains(
            "inspection-wide document count",
            error.Message,
            StringComparison.Ordinal);

        var metadataBounded = new LiteDbMigrationSourceInspector(
            fixture.Path,
            password: null,
            new LiteDbInspectionLimits { MaxMetadataBytes = 10 });
        error = await Assert.ThrowsAsync<LiteDbMigrationException>(
            async () => await InspectAsync(metadataBounded, includeProfile: true));
        Assert.Contains("metadata bytes", error.Message, StringComparison.Ordinal);

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await new LiteDbMigrationSourceInspector(fixture.Path)
                .InspectAsync(
                    new MigrationInspectionRequest
                    {
                        TargetCSharpDbVersion =
                            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                        IncludeProfile = true,
                    },
                    canceled.Token));
    }

    private static async Task<MigrationCatalog> InspectAsync(
        LiteDbMigrationSourceInspector inspector,
        bool includeProfile) =>
        await inspector.InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion =
                    CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = includeProfile,
                ProfileSampleSize = 1,
            },
            Ct);

    private static string? Facet(MigrationCatalogObject item, string name) =>
        item.Facets.SingleOrDefault(
            facet => string.Equals(facet.Name, name, StringComparison.Ordinal))?.Value;

    private sealed record SourceState(
        string Sha256,
        long Length,
        long LastWriteTicks,
        IReadOnlyList<string> Files)
    {
        public static SourceState Read(string path)
        {
            var info = new FileInfo(path);
            string[] files = Directory.GetFiles(
                    Path.GetDirectoryName(path)!,
                    "*",
                    SearchOption.TopDirectoryOnly)
                .Select(Path.GetFileName)
                .Order(StringComparer.Ordinal)
                .ToArray()!;
            return new SourceState(
                Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(path)))
                    .ToLowerInvariant(),
                info.Length,
                info.LastWriteTimeUtc.Ticks,
                files);
        }

    }

    private sealed class LiteDbFixture : IDisposable
    {
        public const string SecretValue = "SECRET-PROFILE-VALUE";

        private readonly string directory;

        private LiteDbFixture(string directory, string path)
        {
            this.directory = directory;
            Path = path;
        }

        public string Path { get; }

        public static LiteDbFixture Create()
        {
            string directory = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                "csharpdb-litedb-tests",
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            string path = System.IO.Path.Combine(directory, "fixture.db");
            using (var database = new LiteDatabase(
                       new ConnectionString
                       {
                           Filename = path,
                           Connection = ConnectionType.Direct,
                       }))
            {
                ILiteCollection<BsonDocument> people =
                    database.GetCollection("People", BsonAutoId.ObjectId);
                people.Insert(
                    new BsonDocument
                    {
                        ["_id"] = 1,
                        ["name"] = "First",
                        ["code"] = "A",
                        ["mixed"] = 42,
                        ["nested"] = new BsonDocument
                        {
                            ["items"] = new BsonArray
                            {
                                new BsonDocument { ["value"] = 1 },
                                new BsonDocument { ["value"] = "one" },
                            },
                        },
                        ["secret"] = SecretValue,
                    });
                people.Insert(
                    new BsonDocument
                    {
                        ["_id"] = "1",
                        ["name"] = "Second",
                        ["code"] = "B",
                        ["mixed"] = "forty-two",
                        ["nested"] = BsonValue.Null,
                    });
                people.EnsureIndex(
                    "ix_name",
                    BsonExpression.Create("$.name"),
                    unique: false);
                people.EnsureIndex(
                    "ux_code",
                    BsonExpression.Create("$.code"),
                    unique: true);
                people.EnsureIndex(
                    "ix_name_lower",
                    BsonExpression.Create("LOWER($.name)"),
                    unique: false);

                database.GetCollection("people", BsonAutoId.Int32)
                    .Insert(new BsonDocument { ["_id"] = 7, ["lower"] = true });

                ILiteCollection<BsonDocument> empty =
                    database.GetCollection("_empty_01", BsonAutoId.Int32);
                empty.Insert(new BsonDocument { ["_id"] = 1 });
                empty.DeleteAll();

                database.GetCollection("_odd_01", BsonAutoId.Guid)
                    .Insert(
                        new BsonDocument
                        {
                            ["_id"] = Guid.Parse(
                                "d676a64c-29bd-4aaf-8f4f-14f5c37dd802"),
                            ["Case"] = 1,
                            ["case"] = 2,
                            ["a/b~c"] = true,
                        });
                database.Checkpoint();
            }
            return new LiteDbFixture(directory, path);
        }

        public static LiteDatabase OpenReadOnly(string path) =>
            new(
                new ConnectionString
                {
                    Filename = path,
                    Connection = ConnectionType.Direct,
                    ReadOnly = true,
                    Upgrade = false,
                });

        public void Dispose()
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }
}
