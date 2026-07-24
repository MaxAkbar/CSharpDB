using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonSnapshotPackageTests
{
    private static string TargetVersion =>
        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion;

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(JsonInputFraming.RootArray)]
    [InlineData(JsonInputFraming.MultipleValues)]
    public async Task WritesDeterministicBytesAndReopensAfterOriginalChangeAndDeletion(
        JsonInputFraming framing)
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = Path.Combine(
            workspace.Root,
            "orders.json");
        string firstPackagePath = Path.Combine(
            workspace.Root,
            "orders-first" + JsonSnapshotPackage.FileExtension);
        string secondPackagePath = Path.Combine(
            workspace.Root,
            "orders-second" + JsonSnapshotPackage.FileExtension);
        string source = Frame(
            framing,
            """{"id":1,"name":"alpha","payload":{"z":1.2300E+004,"a":null}}""",
            """{"id":2,"name":"bravo","payload":[true,"x"]}""",
            """{"id":3,"name":"charlie","payload":{"emoji":"😀"}}""");
        await WriteTextAsync(sourcePath, source);

        JsonSnapshotPackageManifest firstManifest;
        JsonSnapshotPackageManifest secondManifest;
        JsonTableSchemaInferenceResult originalSchema;
        MigrationCatalog originalCatalog;
        JsonSourceSnapshot snapshot =
            await CreateSnapshotAsync(sourcePath, workspace.Root);
        try
        {
            originalSchema = await InferAsync(
                snapshot,
                framing,
                maxProfileRecords: 100,
                logicalSourceIdentity: "functional/orders");
            originalCatalog = originalSchema.CreateCatalog(
                TargetVersion);
            firstManifest = await JsonSnapshotPackage.WriteAsync(
                firstPackagePath,
                snapshot,
                originalSchema,
                TargetVersion,
                Cancellation);
            secondManifest = await JsonSnapshotPackage.WriteAsync(
                secondPackagePath,
                snapshot,
                originalSchema,
                TargetVersion,
                Cancellation);

            // Package publication borrows the caller's snapshot.
            await snapshot.VerifyIntegrityAsync(Cancellation);
            await using JsonStreamingReader reader =
                await originalSchema.Binding.OpenReaderAsync(
                    snapshot,
                    Cancellation);
            Assert.Equal(3, (await CollectAsync(reader.ReadValuesAsync(
                Cancellation))).Count);
        }
        finally
        {
            await snapshot.DisposeAsync();
        }

        byte[] firstBytes = await File.ReadAllBytesAsync(
            firstPackagePath,
            Cancellation);
        byte[] secondBytes = await File.ReadAllBytesAsync(
            secondPackagePath,
            Cancellation);
        Assert.Equal(firstBytes, secondBytes);
        AssertManifestEquivalent(firstManifest, secondManifest);
        Assert.Empty(Directory.EnumerateDirectories(workspace.Root));

        await WriteTextAsync(
            sourcePath,
            Frame(
                framing,
                """{"id":999,"name":"changed","payload":false}"""));
        await using (JsonSnapshotPackageSession changedSourceSession =
            await OpenAsync(
                firstPackagePath,
                workspace.Root,
                firstManifest.ManifestDigest))
        {
            AssertManifestEquivalent(
                firstManifest,
                changedSourceSession.Manifest);
            AssertSchemaEquivalent(
                originalSchema,
                changedSourceSession.Schema);
            AssertCatalogEquivalent(
                originalCatalog,
                changedSourceSession.Catalog);
            await AssertRowsAsync(
                changedSourceSession.DataSource);
        }

        Assert.Empty(Directory.EnumerateDirectories(workspace.Root));
        Assert.Equal(
            firstBytes,
            await File.ReadAllBytesAsync(
                firstPackagePath,
                Cancellation));

        File.Delete(sourcePath);
        await using (JsonSnapshotPackageSession deletedSourceSession =
            await OpenAsync(
                secondPackagePath,
                workspace.Root,
                secondManifest.ManifestDigest))
        {
            AssertManifestEquivalent(
                secondManifest,
                deletedSourceSession.Manifest);
            Assert.Equal(
                originalSchema.SnapshotIdentity,
                deletedSourceSession.Schema.SnapshotIdentity);
            Assert.Equal(
                originalSchema.Source,
                deletedSourceSession.DataSource.Source);
            Assert.Equal(
                secondManifest.CatalogDigest,
                MigrationArtifactSerializer.ComputeCatalogDigest(
                    deletedSourceSession.Catalog));
            await AssertRowsAsync(
                deletedSourceSession.DataSource);
        }

        Assert.Empty(Directory.EnumerateDirectories(workspace.Root));
        Assert.Equal(
            secondBytes,
            await File.ReadAllBytesAsync(
                secondPackagePath,
                Cancellation));
    }

    [Theory]
    [InlineData(JsonInputFraming.RootArray)]
    [InlineData(JsonInputFraming.MultipleValues)]
    public async Task ReopenPreservesSampledInferenceOverridesAndLateTailBehavior(
        JsonInputFraming framing)
    {
        using var workspace = new TemporaryDirectory();
        var inferenceOptions =
            new JsonTableSchemaInferenceOptions
            {
                TableName = "sampled_orders",
                MaxColumns = 8,
                MaxTotalColumnNameBytes = 1_024,
                MaxProfileBytes = 64 * 1_024,
                ColumnOverrides =
                [
                    new JsonTableColumnSchemaOverride
                    {
                        ColumnIndex = 0,
                        ExpectedPropertyName = "id",
                        LogicalType =
                            JsonTableColumnLogicalType.SignedInteger,
                        Nullable = false,
                    },
                    new JsonTableColumnSchemaOverride
                    {
                        ColumnIndex = 1,
                        ExpectedPropertyName = "optional",
                        LogicalType =
                            JsonTableColumnLogicalType.Text,
                        Nullable = true,
                        MissingPolicy =
                            JsonMissingPropertyPolicy.AsNull,
                    },
                    new JsonTableColumnSchemaOverride
                    {
                        ColumnIndex = 2,
                        ExpectedPropertyName = "payload",
                        LogicalType =
                            JsonTableColumnLogicalType.Json,
                        Nullable = false,
                    },
                ],
            };
        PackageOrigin origin = await WritePackageAsync(
            workspace,
            "sampled",
            framing,
            Frame(
                framing,
                """{"id":1,"optional":"alpha","payload":{"b":2,"a":1.2300E+004}}""",
                """{"id":2,"payload":[true,null]}""",
                """{"id":"late","optional":null,"payload":{"x":"tail"}}"""),
            maxProfileRecords: 2,
            inferenceOptions);
        File.Delete(origin.SourcePath);

        Assert.Equal(
            MigrationCoverageKind.Sample,
            origin.Schema.TypeProfileCoverage.Kind);
        Assert.Equal(
            JsonTableOverrideValidationStatus.SampleCompatible,
            origin.Schema.Columns[0].OverrideValidation);
        Assert.Equal(
            JsonMissingPropertyPolicy.AsNull,
            origin.Schema.Columns[1].MissingPolicy);

        await using JsonSnapshotPackageSession session =
            await OpenAsync(
                origin.PackagePath,
                workspace.Root,
                origin.Manifest.ManifestDigest);
        AssertManifestEquivalent(origin.Manifest, session.Manifest);
        AssertSchemaEquivalent(origin.Schema, session.Schema);
        AssertCatalogEquivalent(origin.Catalog, session.Catalog);
        Assert.Equal(
            origin.Schema.Recipe.ColumnOverrides.ToArray(),
            session.Schema.Recipe.ColumnOverrides.ToArray());

        MigrationReadRequest request = ReadRequest(
            session.DataSource,
            [
                JsonMigrationObjectIds.Column(0),
                JsonMigrationObjectIds.Column(1),
                JsonMigrationObjectIds.Column(2),
            ],
            batchSize: 2);
        await using IAsyncEnumerator<MigrationDataBatch> batches =
            session.DataSource
                .ReadAsync(request, Cancellation)
                .GetAsyncEnumerator(Cancellation);
        Assert.True(await batches.MoveNextAsync());
        MigrationDataBatch accepted = batches.Current;
        Assert.Equal(2, accepted.Rows.Count);
        Assert.Equal(
            ["1", "2"],
            accepted.Rows.Select(row =>
                row.Values[0].CanonicalText));
        Assert.Equal(
            MigrationSourceValueKind.Null,
            accepted.Rows[1].Values[1].Kind);
        Assert.Equal(
            """{"b":2,"a":1.2300E+004}""",
            accepted.Rows[0].Values[2].CanonicalText);
        Assert.Equal(
            """[true,null]""",
            accepted.Rows[1].Values[2].CanonicalText);

        MigrationRowRejectedException rejection =
            await Assert.ThrowsAsync<MigrationRowRejectedException>(
                async () => await batches.MoveNextAsync());
        Assert.Equal(
            JsonMigrationDataRules.TypeMismatch,
            rejection.Code);
        Assert.Equal(
            JsonMigrationObjectIds.Column(0),
            rejection.ColumnObjectId);
        Assert.Equal(2, rejection.SourceRowOrdinal);
    }

    [Theory]
    [InlineData(JsonInputFraming.RootArray)]
    [InlineData(JsonInputFraming.MultipleValues)]
    public async Task IndependentReopenSessionsPreserveBatchesCursorsAndResumeSuffixes(
        JsonInputFraming framing)
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin = await WritePackageAsync(
            workspace,
            "replay",
            framing,
            Frame(
                framing,
                """{"id":1,"name":"alpha","payload":{"n":1}}""",
                """{"id":2,"name":"bravo","payload":{"n":2}}""",
                """{"id":3,"name":"charlie","payload":{"n":3}}""",
                """{"id":4,"name":"delta","payload":{"n":4}}""",
                """{"id":5,"name":"echo","payload":{"n":5}}"""),
            maxProfileRecords: 100);
        File.Delete(origin.SourcePath);
        string[] projection =
        [
            JsonMigrationObjectIds.Column(2),
            JsonMigrationObjectIds.Column(0),
            JsonMigrationObjectIds.Column(1),
        ];

        List<MigrationDataBatch> firstBatches;
        string firstBoundary;
        await using (JsonSnapshotPackageSession firstSession =
            await OpenAsync(
                origin.PackagePath,
                workspace.Root,
                origin.Manifest.ManifestDigest))
        {
            MigrationReadRequest request = ReadRequest(
                firstSession.DataSource,
                projection,
                batchSize: 2);
            firstBatches = await CollectAsync(
                firstSession.DataSource.ReadAsync(
                    request,
                    Cancellation));
            Assert.Equal(
                [2, 2, 1],
                firstBatches.Select(batch => batch.Rows.Count));
            firstBoundary = Assert.IsType<string>(
                firstBatches[0].NextCursor);
        }

        Assert.Empty(Directory.EnumerateDirectories(workspace.Root));
        await using (JsonSnapshotPackageSession secondSession =
            await OpenAsync(
                origin.PackagePath,
                workspace.Root,
                origin.Manifest.ManifestDigest))
        {
            MigrationReadRequest request = ReadRequest(
                secondSession.DataSource,
                projection,
                batchSize: 2);
            List<MigrationDataBatch> replay = await CollectAsync(
                secondSession.DataSource.ReadAsync(
                    request,
                    Cancellation));
            AssertBatchSequenceEqual(firstBatches, replay);

            List<MigrationDataBatch> resumed = await CollectAsync(
                secondSession.DataSource.ReadAsync(
                    request with
                    {
                        ResumeCursor = firstBoundary,
                    },
                    Cancellation));
            AssertBatchSequenceEqual(
                firstBatches.Skip(1).ToArray(),
                resumed);
        }

        Assert.Empty(Directory.EnumerateDirectories(workspace.Root));
        Assert.True(File.Exists(origin.PackagePath));
    }

    [Theory]
    [InlineData(JsonInputFraming.RootArray)]
    [InlineData(JsonInputFraming.MultipleValues)]
    public async Task OpenSessionsReadPrivateCopiesAfterPackageRemovalAndCleanIndependently(
        JsonInputFraming framing)
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin = await WritePackageAsync(
            workspace,
            "detached",
            framing,
            Frame(
                framing,
                """{"id":1,"name":"alpha"}""",
                """{"id":2,"name":"bravo"}""",
                """{"id":3,"name":"charlie"}""",
                """{"id":4,"name":"delta"}""",
                """{"id":5,"name":"echo"}"""),
            maxProfileRecords: 100);
        File.Delete(origin.SourcePath);
        Assert.Empty(Directory.EnumerateDirectories(workspace.Root));

        Task<JsonSnapshotPackageSession> firstOpen =
            OpenAsync(
                    origin.PackagePath,
                    workspace.Root,
                    origin.Manifest.ManifestDigest)
                .AsTask();
        Task<JsonSnapshotPackageSession> secondOpen =
            OpenAsync(
                    origin.PackagePath,
                    workspace.Root,
                    origin.Manifest.ManifestDigest)
                .AsTask();
        JsonSnapshotPackageSession[] sessions =
            await Task.WhenAll(firstOpen, secondOpen);
        JsonSnapshotPackageSession first = sessions[0];
        JsonSnapshotPackageSession second = sessions[1];
        bool firstDisposed = false;
        bool secondDisposed = false;
        try
        {
            string[] liveWorkspaces =
                Directory.EnumerateDirectories(workspace.Root)
                    .ToArray();
            Assert.Equal(2, liveWorkspaces.Length);

            File.Delete(origin.PackagePath);
            Assert.False(File.Exists(origin.PackagePath));

            string[] columns =
            [
                JsonMigrationObjectIds.Column(0),
                JsonMigrationObjectIds.Column(1),
            ];
            Task<List<MigrationDataBatch>> firstRead =
                CollectAsync(first.DataSource.ReadAsync(
                    ReadRequest(
                        first.DataSource,
                        columns,
                        batchSize: 2),
                    Cancellation));
            Task<List<MigrationDataBatch>> secondRead =
                CollectAsync(second.DataSource.ReadAsync(
                    ReadRequest(
                        second.DataSource,
                        columns,
                        batchSize: 2),
                    Cancellation));
            List<MigrationDataBatch>[] results =
                await Task.WhenAll(firstRead, secondRead);
            AssertBatchSequenceEqual(results[0], results[1]);
            Assert.Equal(
                ["1", "2", "3", "4", "5"],
                results[0]
                    .SelectMany(batch => batch.Rows)
                    .Select(row => row.Values[0].CanonicalText));

            await first.DisposeAsync();
            firstDisposed = true;
            Assert.Single(
                Directory.EnumerateDirectories(workspace.Root));

            List<MigrationDataBatch> secondReplay =
                await CollectAsync(second.DataSource.ReadAsync(
                    ReadRequest(
                        second.DataSource,
                        columns,
                        batchSize: 2),
                    Cancellation));
            AssertBatchSequenceEqual(results[1], secondReplay);

            await second.DisposeAsync();
            secondDisposed = true;
            Assert.Empty(
                Directory.EnumerateDirectories(workspace.Root));
        }
        finally
        {
            if (!firstDisposed)
                await first.DisposeAsync();
            if (!secondDisposed)
                await second.DisposeAsync();
        }
    }

    private static async ValueTask<PackageOrigin> WritePackageAsync(
        TemporaryDirectory workspace,
        string name,
        JsonInputFraming framing,
        string json,
        int maxProfileRecords,
        JsonTableSchemaInferenceOptions? inferenceOptions = null)
    {
        string sourcePath = Path.Combine(
            workspace.Root,
            name + ".json");
        string packagePath = Path.Combine(
            workspace.Root,
            name + JsonSnapshotPackage.FileExtension);
        await WriteTextAsync(sourcePath, json);

        JsonSourceSnapshot snapshot =
            await CreateSnapshotAsync(sourcePath, workspace.Root);
        try
        {
            JsonTableSchemaInferenceResult schema =
                await InferAsync(
                    snapshot,
                    framing,
                    maxProfileRecords,
                    "functional/" + name,
                    inferenceOptions);
            MigrationCatalog catalog = schema.CreateCatalog(
                TargetVersion);
            JsonSnapshotPackageManifest manifest =
                await JsonSnapshotPackage.WriteAsync(
                    packagePath,
                    snapshot,
                    schema,
                    TargetVersion,
                    Cancellation);
            return new PackageOrigin(
                sourcePath,
                packagePath,
                manifest,
                schema,
                catalog);
        }
        finally
        {
            await snapshot.DisposeAsync();
        }
    }

    private static async ValueTask<JsonSourceSnapshot>
        CreateSnapshotAsync(
            string sourcePath,
            string workspacePath) =>
        await JsonSourceSnapshot.CreateFromFileAsync(
            sourcePath,
            new JsonSourceSnapshotOptions
            {
                WorkspacePath = workspacePath,
                MaxSourceBytes = 1024 * 1024,
            },
            Cancellation);

    private static async ValueTask<JsonTableSchemaInferenceResult>
        InferAsync(
            JsonSourceSnapshot snapshot,
            JsonInputFraming framing,
            int maxProfileRecords,
            string logicalSourceIdentity,
            JsonTableSchemaInferenceOptions? inferenceOptions = null)
    {
        JsonSourceBinding binding =
            await JsonSourceBinding.CreateAsync(
                snapshot,
                new JsonStreamingReaderOptions
                {
                    Framing = framing,
                    MaxValueBytes = 256 * 1024,
                    MaxDepth = 32,
                    MaxPropertiesPerObject = 256,
                    MaxArrayElements = 1_024,
                    MaxTotalNodes = 2_048,
                    MaxPropertyNameBytes = 8 * 1_024,
                    MaxStringBytes = 128 * 1_024,
                    MaxNumberBytes = 8 * 1_024,
                    LeaveOpen = true,
                },
                logicalSourceIdentity,
                Cancellation);
        return await JsonTableSchemaInferer.InferAsync(
            binding,
            snapshot,
            maxProfileRecords,
            inferenceOptions,
            Cancellation);
    }

    private static async ValueTask<JsonSnapshotPackageSession>
        OpenAsync(
            string packagePath,
            string workspacePath,
            string? expectedManifestDigest = null) =>
        await JsonSnapshotPackage.OpenAsync(
            packagePath,
            new JsonSnapshotPackageOpenOptions
            {
                WorkspacePath = workspacePath,
                MaxSourceBytes = 1024 * 1024,
                ExpectedManifestDigest =
                    expectedManifestDigest,
            },
            Cancellation);

    private static MigrationReadRequest ReadRequest(
        JsonMigrationDataSource source,
        IReadOnlyList<string> columns,
        int batchSize) =>
        new()
        {
            SourceObjectId = JsonMigrationObjectIds.Table,
            ColumnObjectIds = columns,
            BatchSize = batchSize,
            MaxBatchBytes = 1024 * 1024,
            MaxValueBytes = 256 * 1024,
            SnapshotToken = source.SnapshotIdentity,
        };

    private static async Task AssertRowsAsync(
        JsonMigrationDataSource source)
    {
        List<MigrationDataBatch> batches = await CollectAsync(
            source.ReadAsync(
                ReadRequest(
                    source,
                    [
                        JsonMigrationObjectIds.Column(2),
                        JsonMigrationObjectIds.Column(0),
                        JsonMigrationObjectIds.Column(1),
                    ],
                    batchSize: 2),
                Cancellation));
        Assert.Equal([2, 1], batches.Select(batch =>
            batch.Rows.Count));
        Assert.Equal(
            [
                """{"z":1.2300E+004,"a":null}""",
                """[true,"x"]""",
                """{"emoji":"😀"}""",
            ],
            batches
                .SelectMany(batch => batch.Rows)
                .Select(row => row.Values[0].CanonicalText));
        Assert.Equal(
            ["1", "2", "3"],
            batches
                .SelectMany(batch => batch.Rows)
                .Select(row => row.Values[1].CanonicalText));
        Assert.Equal(
            ["alpha", "bravo", "charlie"],
            batches
                .SelectMany(batch => batch.Rows)
                .Select(row => row.Values[2].CanonicalText));
    }

    private static string Frame(
        JsonInputFraming framing,
        params string[] values) =>
        framing switch
        {
            JsonInputFraming.RootArray =>
                "[\n" +
                string.Join(",\n", values) +
                "\n]",
            JsonInputFraming.MultipleValues =>
                string.Join("\n", values) + "\n",
            _ => throw new ArgumentOutOfRangeException(
                nameof(framing)),
        };

    private static void AssertManifestEquivalent(
        JsonSnapshotPackageManifest expected,
        JsonSnapshotPackageManifest actual)
    {
        Assert.Equal(
            expected.ManifestDigest,
            actual.ManifestDigest);
        Assert.Equal(
            expected.SnapshotIdentity,
            actual.SnapshotIdentity);
        Assert.Equal(
            expected.ContentDigest,
            actual.ContentDigest);
        Assert.Equal(
            expected.ContentLength,
            actual.ContentLength);
        Assert.Equal(expected.Source, actual.Source);
        Assert.Equal(
            expected.OptionsDigest,
            actual.OptionsDigest);
        Assert.Equal(
            expected.TargetCSharpDbVersion,
            actual.TargetCSharpDbVersion);
        Assert.Equal(
            expected.CatalogDigest,
            actual.CatalogDigest);
    }

    private static void AssertCatalogEquivalent(
        MigrationCatalog expected,
        MigrationCatalog actual) =>
        Assert.Equal(
            MigrationArtifactSerializer.SerializeCatalog(
                expected,
                writeIndented: false),
            MigrationArtifactSerializer.SerializeCatalog(
                actual,
                writeIndented: false));

    private static void AssertSchemaEquivalent(
        JsonTableSchemaInferenceResult expected,
        JsonTableSchemaInferenceResult actual)
    {
        Assert.Equal(expected.Source, actual.Source);
        Assert.Equal(
            expected.SnapshotIdentity,
            actual.SnapshotIdentity);
        Assert.Equal(
            expected.ContentDigest,
            actual.ContentDigest);
        Assert.Equal(
            expected.ContentLength,
            actual.ContentLength);
        Assert.Equal(expected.TableName, actual.TableName);
        Assert.Equal(
            expected.TotalRecords,
            actual.TotalRecords);
        Assert.Equal(
            expected.EligibleObjectRecords,
            actual.EligibleObjectRecords);
        Assert.Equal(
            expected.IneligibleRecords,
            actual.IneligibleRecords);
        Assert.Equal(
            expected.TotalColumnNameBytes,
            actual.TotalColumnNameBytes);
        Assert.Equal(
            expected.ProfileRecordsExamined,
            actual.ProfileRecordsExamined);
        Assert.Equal(
            expected.ProfileBytesExamined,
            actual.ProfileBytesExamined);
        Assert.Equal(
            expected.ProfileRecordLimitReached,
            actual.ProfileRecordLimitReached);
        Assert.Equal(
            expected.ProfileByteLimitReached,
            actual.ProfileByteLimitReached);
        Assert.Equal(
            expected.StructuralCoverage,
            actual.StructuralCoverage);
        Assert.Equal(
            expected.TypeProfileCoverage,
            actual.TypeProfileCoverage);
        Assert.Equal(
            expected.Binding.ReaderOptions,
            actual.Binding.ReaderOptions);
        Assert.Equal(
            expected.Recipe.CollectProfile,
            actual.Recipe.CollectProfile);
        Assert.Equal(
            expected.Recipe.MaxProfileRecords,
            actual.Recipe.MaxProfileRecords);
        Assert.Equal(
            expected.Recipe.TableName,
            actual.Recipe.TableName);
        Assert.Equal(
            expected.Recipe.MaxColumns,
            actual.Recipe.MaxColumns);
        Assert.Equal(
            expected.Recipe.MaxTotalColumnNameBytes,
            actual.Recipe.MaxTotalColumnNameBytes);
        Assert.Equal(
            expected.Recipe.MaxProfileBytes,
            actual.Recipe.MaxProfileBytes);
        Assert.Equal(
            expected.Recipe.ColumnOverrides.ToArray(),
            actual.Recipe.ColumnOverrides.ToArray());
        Assert.Equal(
            expected.Columns.Count,
            actual.Columns.Count);
        for (int index = 0;
             index < expected.Columns.Count;
             index++)
        {
            JsonTableColumnSchema left =
                expected.Columns[index];
            JsonTableColumnSchema right =
                actual.Columns[index];
            Assert.Equal(
                left.ColumnIndex,
                right.ColumnIndex);
            Assert.Equal(left.SourceName, right.SourceName);
            Assert.Equal(
                left.OriginalPropertyName,
                right.OriginalPropertyName);
            Assert.Equal(
                left.FirstSeenRecordOrdinal,
                right.FirstSeenRecordOrdinal);
            Assert.Equal(
                left.FirstSeenPropertyOrdinal,
                right.FirstSeenPropertyOrdinal);
            Assert.Equal(
                left.LogicalType,
                right.LogicalType);
            Assert.Equal(
                left.Resolution,
                right.Resolution);
            Assert.Equal(left.Reason, right.Reason);
            Assert.Equal(
                left.Confidence,
                right.Confidence);
            Assert.Equal(left.Nullable, right.Nullable);
            Assert.Equal(
                left.MissingPolicy,
                right.MissingPolicy);
            Assert.Equal(
                left.OverrideValidation,
                right.OverrideValidation);
            Assert.Equal(
                left.PresentCount,
                right.PresentCount);
            Assert.Equal(left.NullCount, right.NullCount);
            Assert.Equal(
                left.MissingCount,
                right.MissingCount);
            Assert.Equal(
                left.ProfiledNonNullCount,
                right.ProfiledNonNullCount);
            Assert.Equal(
                left.ProfiledStringCount,
                right.ProfiledStringCount);
            Assert.Equal(
                left.ProfiledBooleanCount,
                right.ProfiledBooleanCount);
            Assert.Equal(
                left.ProfiledNumberCount,
                right.ProfiledNumberCount);
            Assert.Equal(
                left.ProfiledObjectCount,
                right.ProfiledObjectCount);
            Assert.Equal(
                left.ProfiledArrayCount,
                right.ProfiledArrayCount);
            Assert.Equal(
                left.ProfiledLexemePreservationCount,
                right.ProfiledLexemePreservationCount);
            Assert.Equal(
                left.ObservedMaxCanonicalValueBytes,
                right.ObservedMaxCanonicalValueBytes);
            Assert.Equal(
                left.ObservedPrecision,
                right.ObservedPrecision);
            Assert.Equal(
                left.ObservedScale,
                right.ObservedScale);
            Assert.Equal(
                left.FirstOverrideMismatchRecordOrdinal,
                right.FirstOverrideMismatchRecordOrdinal);
        }

        Assert.Equal(
            expected.Diagnostics.Select(item =>
                (
                    item.DiagnosticId,
                    item.RuleId,
                    item.Severity,
                    item.ObjectId)),
            actual.Diagnostics.Select(item =>
                (
                    item.DiagnosticId,
                    item.RuleId,
                    item.Severity,
                    item.ObjectId)));
    }

    private static void AssertBatchSequenceEqual(
        IReadOnlyList<MigrationDataBatch> expected,
        IReadOnlyList<MigrationDataBatch> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (int index = 0; index < expected.Count; index++)
            AssertBatchEqual(expected[index], actual[index]);
    }

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
        Assert.Equal(expected.Rows.Count, actual.Rows.Count);
        for (int rowIndex = 0;
             rowIndex < expected.Rows.Count;
             rowIndex++)
        {
            MigrationDataRow left = expected.Rows[rowIndex];
            MigrationDataRow right = actual.Rows[rowIndex];
            Assert.Equal(left.StableKey, right.StableKey);
            Assert.Equal(
                left.Values.Count,
                right.Values.Count);
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
                    left.Values[valueIndex]
                        .BinaryValue
                        .ToArray(),
                    right.Values[valueIndex]
                        .BinaryValue
                        .ToArray());
            }
        }

        Assert.Equal(
            expected.RejectedRows.Count,
            actual.RejectedRows.Count);
        for (int index = 0;
             index < expected.RejectedRows.Count;
             index++)
        {
            MigrationRejectedRow left =
                expected.RejectedRows[index];
            MigrationRejectedRow right =
                actual.RejectedRows[index];
            Assert.Equal(
                left.SourceRowOrdinal,
                right.SourceRowOrdinal);
            Assert.Equal(left.RuleId, right.RuleId);
            Assert.Equal(
                left.ColumnObjectId,
                right.ColumnObjectId);
            Assert.Equal(
                left.Evidence.Select(item =>
                    (item.Name, item.Value)),
                right.Evidence.Select(item =>
                    (item.Name, item.Value)));
        }
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

    private static async ValueTask WriteTextAsync(
        string path,
        string contents) =>
        await File.WriteAllTextAsync(
            path,
            contents,
            new UTF8Encoding(false, true),
            Cancellation);

    private sealed record PackageOrigin(
        string SourcePath,
        string PackagePath,
        JsonSnapshotPackageManifest Manifest,
        JsonTableSchemaInferenceResult Schema,
        MigrationCatalog Catalog);

    private sealed class TemporaryDirectory : IDisposable
    {
        internal TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                "csharpdb-json-package-tests-" +
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        public void Dispose()
        {
            if (Directory.Exists(Root))
            {
                Directory.Delete(
                    Root,
                    recursive: true);
            }
        }
    }
}
