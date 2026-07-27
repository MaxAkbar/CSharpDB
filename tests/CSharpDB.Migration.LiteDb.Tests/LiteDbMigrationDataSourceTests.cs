using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.LiteDb;
using LiteDB;

namespace CSharpDB.Migration.LiteDb.Tests;

public sealed class LiteDbMigrationDataSourceTests
{
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task StreamsTypedKeysAndCanonicalDocumentsInIdIndexOrder()
    {
        using var temporary = new LiteDbTestDirectory();
        await using OpenedSource source =
            await OpenedSource.CreateAsync(
                temporary,
                "exact-values",
                includeProfile: true);
        MigrationCatalogObject collection =
            source.Collection("records");
        MigrationReadRequest request =
            source.Request(collection, batchSize: 2);

        IReadOnlyList<MigrationDataBatch> batches =
            await ReadAllAsync(
                source.DataSource,
                request,
                Ct);
        MigrationDataRow[] rows = batches
            .SelectMany(static batch => batch.Rows)
            .ToArray();
        ExpectedRow[] expected =
            ReadExpectedRows(
                source.Snapshot.FilePath,
                collection.SourceName);

        Assert.Equal(
            expected.Select(static row => row.Key),
            rows.Select(static row => row.StableKey));
        Assert.Equal(
            expected.Select(static row => row.Document),
            rows.Select(static row =>
                row.Values[1].CanonicalText));
        Assert.Equal(
            expected.Length,
            rows.Select(static row => row.StableKey)
                .Distinct(StringComparer.Ordinal)
                .Count());
        Assert.All(rows, static row =>
        {
            Assert.Equal(2, row.Values.Count);
            Assert.Equal(
                MigrationSourceValueKind.Text,
                row.Values[0].Kind);
            Assert.Equal(
                MigrationSourceValueKind.Json,
                row.Values[1].Kind);
            Assert.Equal(
                row.StableKey,
                row.Values[0].CanonicalText);
            Assert.True(
                MigrationLiteDbDocumentCollectionContract
                    .TryValidateTypedKey(
                        row.StableKey,
                        out string? reason),
                reason);
            Assert.Contains(
                "\"name\":\"_id\"",
                row.Values[1].CanonicalText,
                StringComparison.Ordinal);
        });

        string[] expectedColumns =
            source.Columns(collection)
                .Select(static item => item.ObjectId)
                .ToArray();
        Assert.All(batches, batch =>
        {
            Assert.Equal(
                collection.ObjectId,
                batch.SourceObjectId);
            Assert.Equal(
                source.Snapshot.SnapshotIdentity,
                batch.SnapshotIdentity);
            Assert.Equal(
                expectedColumns,
                batch.ColumnObjectIds);
            Assert.Empty(batch.RejectedRows);
        });
        Assert.Equal(
            [0L, 1L, 2L],
            batches.Select(static batch =>
                batch.BatchOrdinal));
        Assert.Null(batches[0].StartCursor);
        Assert.Equal(
            batches[0].NextCursor,
            batches[1].StartCursor);
        Assert.Equal(
            batches[1].NextCursor,
            batches[2].StartCursor);
        Assert.Null(batches[2].NextCursor);
    }

    [Fact]
    public async Task AlignsValuesToEitherExactBridgeColumnOrder()
    {
        using var temporary = new LiteDbTestDirectory();
        await using OpenedSource source =
            await OpenedSource.CreateAsync(
                temporary,
                "bridge-order");
        MigrationCatalogObject collection =
            source.Collection("records");
        MigrationCatalogObject[] bridge =
            source.Columns(collection);
        MigrationCatalogObject key = Assert.Single(
            bridge,
            static item =>
                item.SourceName ==
                MigrationLiteDbDocumentCollectionContract
                    .KeyColumnName);
        MigrationCatalogObject document = Assert.Single(
            bridge,
            static item =>
                item.SourceName ==
                MigrationLiteDbDocumentCollectionContract
                    .DocumentColumnName);
        string[] executorOrder = bridge
            .Select(static item => item.ObjectId)
            .Order(StringComparer.Ordinal)
            .ToArray();
        string[][] orders =
        [
            executorOrder,
            executorOrder.Reverse().ToArray(),
        ];

        foreach (string[] order in orders)
        {
            MigrationReadRequest request =
                source.Request(
                    collection,
                    batchSize: 3) with
                {
                    ColumnObjectIds = order,
                };
            IReadOnlyList<MigrationDataBatch> batches =
                await ReadAllAsync(
                    source.DataSource,
                    request,
                    Ct);

            Assert.All(
                batches,
                batch => Assert.Equal(
                    order,
                    batch.ColumnObjectIds));
            foreach (MigrationDataRow row in batches
                         .SelectMany(
                             static batch => batch.Rows))
            {
                int keyIndex = Array.IndexOf(
                    order,
                    key.ObjectId);
                int documentIndex = Array.IndexOf(
                    order,
                    document.ObjectId);
                Assert.Equal(
                    MigrationSourceValueKind.Text,
                    row.Values[keyIndex].Kind);
                Assert.Equal(
                    MigrationSourceValueKind.Json,
                    row.Values[documentIndex].Kind);
                Assert.Equal(
                    row.StableKey,
                    row.Values[keyIndex].CanonicalText);
                Assert.Contains(
                    "\"name\":\"_id\"",
                    row.Values[documentIndex]
                        .CanonicalText,
                    StringComparison.Ordinal);
            }
        }
    }

    [Fact]
    public async Task SplitsAtRowAndCanonicalByteBounds()
    {
        using var temporary = new LiteDbTestDirectory();
        await using OpenedSource source =
            await OpenedSource.CreateAsync(
                temporary,
                "bounds");
        MigrationCatalogObject collection =
            source.Collection("records");

        IReadOnlyList<MigrationDataBatch> rowBounded =
            await ReadAllAsync(
                source.DataSource,
                source.Request(
                    collection,
                    batchSize: 2),
                Ct);
        Assert.Equal(
            [2, 2, 1],
            rowBounded.Select(static batch =>
                batch.Rows.Count));

        MigrationDataRow[] unboundedRows = rowBounded
            .SelectMany(static batch => batch.Rows)
            .ToArray();
        long maximumRowBytes = unboundedRows.Max(
            static row =>
                Utf8Bytes(row.Values[0].CanonicalText!) +
                Utf8Bytes(row.Values[1].CanonicalText!));
        int maximumValueBytes = checked(
            (int)unboundedRows.Max(
                static row => Math.Max(
                    Utf8Bytes(
                        row.Values[0].CanonicalText!),
                    Utf8Bytes(
                        row.Values[1].CanonicalText!))));

        IReadOnlyList<MigrationDataBatch> byteBounded =
            await ReadAllAsync(
                source.DataSource,
                source.Request(
                    collection,
                    batchSize: 100,
                    maxBatchBytes: maximumRowBytes,
                    maxValueBytes: maximumValueBytes),
                Ct);

        Assert.Equal(
            [1, 1, 1, 1, 1],
            byteBounded.Select(static batch =>
                batch.Rows.Count));
        Assert.All(byteBounded, batch =>
        {
            long bytes = batch.Rows.Sum(
                static row =>
                    Utf8Bytes(
                        row.Values[0].CanonicalText!) +
                    Utf8Bytes(
                        row.Values[1].CanonicalText!));
            Assert.InRange(
                bytes,
                1,
                maximumRowBytes);
        });
    }

    [Fact]
    public async Task EveryEmittedCursorResumesWithoutDuplicatesOrMissingRows()
    {
        using var temporary = new LiteDbTestDirectory();
        await using OpenedSource source =
            await OpenedSource.CreateAsync(
                temporary,
                "resume");
        MigrationCatalogObject collection =
            source.Collection("records");
        MigrationReadRequest request =
            source.Request(
                collection,
                batchSize: 2);
        IReadOnlyList<MigrationDataBatch> original =
            await ReadAllAsync(
                source.DataSource,
                request,
                Ct);
        string[] expected = original
            .SelectMany(static batch => batch.Rows)
            .Select(static row =>
                Assert.IsType<string>(row.StableKey))
            .ToArray();

        for (int boundary = 0;
             boundary < original.Count - 1;
             boundary++)
        {
            string cursor = Assert.IsType<string>(
                original[boundary].NextCursor);
            IReadOnlyList<MigrationDataBatch> resumed =
                await ReadAllAsync(
                    source.DataSource,
                    request with
                    {
                        ResumeCursor = cursor,
                    },
                    Ct);
            int rowsBeforeBoundary = original
                .Take(boundary + 1)
                .Sum(static batch => batch.Rows.Count);

            Assert.Equal(
                expected[rowsBeforeBoundary..],
                resumed
                    .SelectMany(static batch => batch.Rows)
                    .Select(static row => row.StableKey));
            Assert.Equal(
                boundary + 1,
                resumed[0].BatchOrdinal);
            Assert.Equal(
                cursor,
                resumed[0].StartCursor);
        }
    }

    [Fact]
    public async Task RejectsTamperedCrossObjectCrossCatalogAndCrossSnapshotCursors()
    {
        using var temporary = new LiteDbTestDirectory();
        await using OpenedSource source =
            await OpenedSource.CreateAsync(
                temporary,
                "cursor-first",
                includeProfile: false);
        await using OpenedSource otherSnapshot =
            await OpenedSource.CreateAsync(
                temporary,
                "cursor-second",
                includeProfile: false,
                addExtraRecord: true);

        MigrationCatalogObject records =
            source.Collection("records");
        MigrationReadRequest request =
            source.Request(
                records,
                batchSize: 1);
        IReadOnlyList<MigrationDataBatch> batches =
            await ReadAllAsync(
                source.DataSource,
                request,
                Ct);
        string cursor = Assert.IsType<string>(
            batches[0].NextCursor);
        string tampered =
            cursor[..^1] +
            (cursor[^1] == 'a' ? 'b' : 'a');

        Assert.Throws<InvalidDataException>(
            () => source.DataSource.ReadAsync(
                request with
                {
                    ResumeCursor = tampered,
                },
                Ct));

        MigrationCatalogObject otherCollection =
            source.Collection("other");
        Assert.Throws<InvalidDataException>(
            () => source.DataSource.ReadAsync(
                source.Request(
                    otherCollection,
                    batchSize: 1) with
                {
                    ResumeCursor = cursor,
                },
                Ct));

        Assert.Throws<InvalidDataException>(
            () => source.DataSource.ReadAsync(
                request with
                {
                    BatchSize = 2,
                    ResumeCursor = cursor,
                },
                Ct));

        MigrationCatalog profiledCatalog =
            await InspectAsync(
                source.Snapshot,
                includeProfile: true);
        await using LiteDbMigrationDataSource
            otherCatalogDataSource =
                await LiteDbMigrationDataSource.CreateAsync(
                    source.Snapshot,
                    profiledCatalog,
                    Ct);
        MigrationCatalogObject profiledRecords =
            Collection(
                profiledCatalog,
                "records");
        Assert.Throws<InvalidDataException>(
            () => otherCatalogDataSource.ReadAsync(
                Request(
                    source.Snapshot,
                    profiledCatalog,
                    profiledRecords,
                    batchSize: 1) with
                {
                    ResumeCursor = cursor,
                },
                Ct));

        MigrationCatalogObject otherSnapshotRecords =
            otherSnapshot.Collection("records");
        Assert.Throws<InvalidDataException>(
            () => otherSnapshot.DataSource.ReadAsync(
                otherSnapshot.Request(
                    otherSnapshotRecords,
                    batchSize: 1) with
                {
                    ResumeCursor = cursor,
                },
                Ct));

        Assert.Throws<InvalidDataException>(
            () => source.DataSource.ReadAsync(
                request with
                {
                    SnapshotToken =
                        otherSnapshot.Snapshot
                            .SnapshotIdentity,
                },
                Ct));
    }

    [Fact]
    public async Task RejectsMismatchedCatalogBoundsCancellationAndUseAfterDispose()
    {
        using var temporary = new LiteDbTestDirectory();
        await using OpenedSource first =
            await OpenedSource.CreateAsync(
                temporary,
                "binding-first");
        await using OpenedSource second =
            await OpenedSource.CreateAsync(
                temporary,
                "binding-second",
                addExtraRecord: true);

        await Assert.ThrowsAsync<ArgumentException>(
            async () =>
            {
                _ = await LiteDbMigrationDataSource
                    .CreateAsync(
                        first.Snapshot,
                        second.Catalog,
                        Ct);
            });

        MigrationCatalogObject records =
            first.Collection("records");
        MigrationReadRequest normal =
            first.Request(records);
        MigrationDataRow row = Assert.Single(
            (await ReadAllAsync(
                first.DataSource,
                normal with
                {
                    BatchSize = 1,
                },
                Ct))[0].Rows);
        int documentBytes = checked(
            (int)Utf8Bytes(
                row.Values[1].CanonicalText!));
        long rowBytes =
            Utf8Bytes(
                row.Values[0].CanonicalText!) +
            documentBytes;

        InvalidDataException valueError =
            await Assert.ThrowsAsync<InvalidDataException>(
                async () =>
                {
                    _ = await ReadAllAsync(
                        first.DataSource,
                        normal with
                        {
                            MaxBatchBytes =
                                rowBytes,
                            MaxValueBytes =
                                documentBytes - 1,
                        },
                        Ct);
                });
        Assert.StartsWith(
            LiteDbMigrationDataRules.ValueSizeExceeded,
            valueError.Message,
            StringComparison.Ordinal);

        InvalidDataException rowError =
            await Assert.ThrowsAsync<InvalidDataException>(
                async () =>
                {
                    _ = await ReadAllAsync(
                        first.DataSource,
                        normal with
                        {
                            MaxBatchBytes =
                                rowBytes - 1,
                            MaxValueBytes =
                                documentBytes,
                        },
                        Ct);
                });
        Assert.StartsWith(
            LiteDbMigrationDataRules.RowSizeExceeded,
            rowError.Message,
            StringComparison.Ordinal);

        using var canceled =
            new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAnyAsync<
            OperationCanceledException>(
            async () =>
            {
                _ = await ReadAllAsync(
                    first.DataSource,
                    normal,
                    canceled.Token);
            });

        LiteDbMigrationDataSource disposed =
            await LiteDbMigrationDataSource.CreateAsync(
                first.Snapshot,
                first.Catalog,
                Ct);
        await disposed.DisposeAsync();
        Assert.Throws<ObjectDisposedException>(
            () => disposed.ReadAsync(normal, Ct));
    }

    private static async Task<
        IReadOnlyList<MigrationDataBatch>> ReadAllAsync(
        IMigrationDataSource source,
        MigrationReadRequest request,
        CancellationToken cancellationToken)
    {
        var batches = new List<MigrationDataBatch>();
        await foreach (MigrationDataBatch batch in source
                           .ReadAsync(
                               request,
                               cancellationToken)
                           .WithCancellation(
                               cancellationToken))
        {
            batches.Add(batch);
        }

        return batches;
    }

    private static async ValueTask<MigrationCatalog>
        InspectAsync(
            LiteDbRetainedSnapshot snapshot,
            bool includeProfile) =>
        await new LiteDbMigrationSourceInspector(snapshot)
            .InspectAsync(
                new MigrationInspectionRequest
                {
                    TargetCSharpDbVersion =
                        CSharpDbCapabilityCatalogLoader
                            .CurrentTargetVersion,
                    IncludeProfile = includeProfile,
                    ProfileSampleSize = 100,
                },
                Ct);

    private static MigrationCatalogObject Collection(
        MigrationCatalog catalog,
        string sourceName) =>
        Assert.Single(
            catalog.Objects,
            item =>
                item.Kind ==
                    MigrationObjectKind.Collection &&
                item.SourceName == sourceName);

    private static MigrationCatalogObject[] Columns(
        MigrationCatalog catalog,
        MigrationCatalogObject collection)
    {
        IReadOnlyDictionary<string, MigrationCatalogObject>
            objects = catalog.Objects.ToDictionary(
                static item => item.ObjectId,
                StringComparer.Ordinal);
        Assert.True(
            MigrationLiteDbDocumentCollectionContract
                .TryBindExactV1Collection(
                    collection,
                    objects,
                    out MigrationCatalogObject? key,
                    out MigrationCatalogObject? document,
                    out string? reason),
            reason);
        return [key!, document!];
    }

    private static MigrationReadRequest Request(
        LiteDbRetainedSnapshot snapshot,
        MigrationCatalog catalog,
        MigrationCatalogObject collection,
        int batchSize = 1_000,
        long maxBatchBytes = 1024 * 1024,
        int maxValueBytes = 256 * 1024) =>
        new()
        {
            SourceObjectId =
                collection.ObjectId,
            ColumnObjectIds =
                Columns(catalog, collection)
                    .Select(static item =>
                        item.ObjectId)
                    .ToArray(),
            BatchSize = batchSize,
            MaxBatchBytes = maxBatchBytes,
            MaxValueBytes = maxValueBytes,
            SnapshotToken =
                snapshot.SnapshotIdentity,
        };

    private static ExpectedRow[] ReadExpectedRows(
        string snapshotPath,
        string collectionName)
    {
        using var database = new LiteDatabase(
            new ConnectionString
            {
                Filename = snapshotPath,
                Connection = ConnectionType.Direct,
                ReadOnly = true,
                Upgrade = false,
            });
        return database
            .GetCollection(
                collectionName,
                BsonAutoId.ObjectId)
            .Find(
                Query.All(
                    "_id",
                    Query.Ascending),
                0,
                int.MaxValue)
            .Select(static document =>
                new ExpectedRow(
                    LiteDbCanonicalBsonCodec
                        .EncodeTypedKey(
                            document["_id"]),
                    LiteDbCanonicalBsonCodec
                        .EncodeDocument(
                            document)))
            .ToArray();
    }

    private static long Utf8Bytes(string value) =>
        Encoding.UTF8.GetByteCount(value);

    private sealed record ExpectedRow(
        string Key,
        string Document);

    private sealed class OpenedSource :
        IAsyncDisposable
    {
        private OpenedSource(
            LiteDbRetainedSnapshot snapshot,
            MigrationCatalog catalog,
            LiteDbMigrationDataSource dataSource)
        {
            Snapshot = snapshot;
            Catalog = catalog;
            DataSource = dataSource;
        }

        internal LiteDbRetainedSnapshot Snapshot
        {
            get;
        }

        internal MigrationCatalog Catalog
        {
            get;
        }

        internal LiteDbMigrationDataSource DataSource
        {
            get;
        }

        internal static async ValueTask<OpenedSource>
            CreateAsync(
                LiteDbTestDirectory temporary,
                string prefix,
                bool includeProfile = false,
                bool addExtraRecord = false)
        {
            string sourcePath = temporary.PathFor(
                prefix + "-source.db");
            string snapshotPath = temporary.PathFor(
                prefix + "-snapshot.db");
            CreateDatabase(
                sourcePath,
                addExtraRecord);
            LiteDbRetainedSnapshot snapshot =
                await LiteDbRetainedSnapshot
                    .CreateAsync(
                        sourcePath,
                        snapshotPath,
                        Ct);
            MigrationCatalog catalog =
                await InspectAsync(
                    snapshot,
                    includeProfile);
            LiteDbMigrationDataSource dataSource =
                await LiteDbMigrationDataSource
                    .CreateAsync(
                        snapshot,
                        catalog,
                        Ct);
            return new OpenedSource(
                snapshot,
                catalog,
                dataSource);
        }

        internal MigrationCatalogObject Collection(
            string sourceName) =>
            LiteDbMigrationDataSourceTests.Collection(
                Catalog,
                sourceName);

        internal MigrationCatalogObject[] Columns(
            MigrationCatalogObject collection) =>
            LiteDbMigrationDataSourceTests.Columns(
                Catalog,
                collection);

        internal MigrationReadRequest Request(
            MigrationCatalogObject collection,
            int batchSize = 1_000,
            long maxBatchBytes =
                1024 * 1024,
            int maxValueBytes =
                256 * 1024) =>
            LiteDbMigrationDataSourceTests.Request(
                Snapshot,
                Catalog,
                collection,
                batchSize,
                maxBatchBytes,
                maxValueBytes);

        public ValueTask DisposeAsync() =>
            DataSource.DisposeAsync();
    }

    private sealed class LiteDbTestDirectory :
        IDisposable
    {
        internal LiteDbTestDirectory()
        {
            Path = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                "csharpdb-litedb-data-tests",
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Path);
        }

        internal string Path { get; }

        internal string PathFor(string fileName) =>
            System.IO.Path.Combine(
                Path,
                fileName);

        public void Dispose()
        {
            if (Directory.Exists(Path))
            {
                Directory.Delete(
                    Path,
                    recursive: true);
            }
        }
    }

    private static void CreateDatabase(
        string path,
        bool addExtraRecord)
    {
        using var database = new LiteDatabase(
            new ConnectionString
            {
                Filename = path,
                Connection = ConnectionType.Direct,
            });
        ILiteCollection<BsonDocument> records =
            database.GetCollection(
                "records",
                BsonAutoId.ObjectId);
        records.Insert(
            new BsonDocument
            {
                ["_id"] = "middle",
                ["payload"] = "string-id",
            });
        records.Insert(
            new BsonDocument
            {
                ["_id"] = 40,
                ["payload"] = "forty",
                ["nested"] = new BsonDocument
                {
                    ["z"] = true,
                    ["a"] = 1,
                },
            });
        records.Insert(
            new BsonDocument
            {
                ["_id"] = -7,
                ["payload"] = "negative",
            });
        records.Insert(
            new BsonDocument
            {
                ["_id"] =
                    new ObjectId(
                        "64b7f56a9519a7c245e8b001"),
                ["payload"] =
                    new BsonArray
                    {
                        1,
                        "two",
                        BsonValue.Null,
                    },
            });
        records.Insert(
            new BsonDocument
            {
                ["_id"] = Guid.Parse(
                    "d676a64c-29bd-4aaf-8f4f-14f5c37dd802"),
                ["payload"] =
                    new byte[]
                    {
                        0x00,
                        0x01,
                        0xFE,
                        0xFF,
                    },
            });
        if (addExtraRecord)
        {
            records.Insert(
                new BsonDocument
                {
                    ["_id"] = 900,
                    ["payload"] = "extra",
                });
        }

        database.GetCollection(
                "other",
                BsonAutoId.Int32)
            .Insert(
                new BsonDocument
                {
                    ["_id"] = 1,
                    ["payload"] = "other",
                });
        database.Checkpoint();
    }
}
