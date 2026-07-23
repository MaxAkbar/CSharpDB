using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Csv;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvSnapshotPackageTests
{
    private static string TargetVersion => CSharpDbCapabilityCatalogLoader.CurrentTargetVersion;

    [Fact]
    public async Task WritesDeterministicBytesAndReopensAfterOriginalChangeAndDeletion()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = Path.Combine(workspace.Root, "orders.csv");
        string firstPackagePath = Path.Combine(workspace.Root, "orders-first.csdbcsv");
        string secondPackagePath = Path.Combine(workspace.Root, "orders-second.csdbcsv");
        const string csv =
            "id,name,amount\n" +
            "1,alpha,1.25\n" +
            "2,\"bravo, incorporated\",2.50\n" +
            "3,charlie,3.75\n";
        await WriteTextAsync(sourcePath, csv);

        CsvSnapshotPackageManifest firstManifest;
        CsvSnapshotPackageManifest secondManifest;
        CsvSchemaInferenceResult originalSchema;
        MigrationCatalog originalCatalog;
        CsvSourceSnapshot snapshot = await CreateSnapshotAsync(sourcePath, workspace.Root);
        try
        {
            originalSchema = await InferAsync(
                snapshot,
                maxDataRecords: 100,
                logicalSourceIdentity: "functional/orders");
            originalCatalog = originalSchema.CreateCatalog(TargetVersion);
            firstManifest = await CsvSnapshotPackage.WriteAsync(
                firstPackagePath,
                snapshot,
                originalSchema,
                TargetVersion,
                Cancellation);
            secondManifest = await CsvSnapshotPackage.WriteAsync(
                secondPackagePath,
                snapshot,
                originalSchema,
                TargetVersion,
                Cancellation);
        }
        finally
        {
            await snapshot.DisposeAsync();
        }

        byte[] firstBytes = await File.ReadAllBytesAsync(firstPackagePath, Cancellation);
        byte[] secondBytes = await File.ReadAllBytesAsync(secondPackagePath, Cancellation);
        Assert.Equal(firstBytes, secondBytes);
        AssertManifestEquivalent(firstManifest, secondManifest);

        await WriteTextAsync(sourcePath, "id,name,amount\n999,changed,999.00\n");
        await using (CsvSnapshotPackageSession changedSourceSession =
            await OpenAsync(firstPackagePath, workspace.Root))
        {
            AssertManifestEquivalent(firstManifest, changedSourceSession.Manifest);
            Assert.Equal(originalSchema.Source, changedSourceSession.Schema.Source);
            Assert.Equal(originalCatalog.Source, changedSourceSession.Catalog.Source);
            AssertCatalogEquivalent(originalCatalog, changedSourceSession.Catalog);
        }

        Assert.True(File.Exists(firstPackagePath));
        Assert.Equal(firstBytes, await File.ReadAllBytesAsync(firstPackagePath, Cancellation));

        File.Delete(sourcePath);
        Assert.False(File.Exists(sourcePath));
        await using (CsvSnapshotPackageSession deletedSourceSession =
            await OpenAsync(secondPackagePath, workspace.Root))
        {
            AssertManifestEquivalent(secondManifest, deletedSourceSession.Manifest);
            Assert.Equal(originalSchema.SnapshotIdentity, deletedSourceSession.Schema.SnapshotIdentity);
            Assert.Equal(originalSchema.Source, deletedSourceSession.DataSource.Source);
            Assert.Equal(
                firstManifest.CatalogDigest,
                MigrationArtifactSerializer.ComputeCatalogDigest(deletedSourceSession.Catalog));
        }

        Assert.True(File.Exists(secondPackagePath));
        Assert.Equal(secondBytes, await File.ReadAllBytesAsync(secondPackagePath, Cancellation));
    }

    [Fact]
    public async Task IndependentReopenSessionsPreserveBatchesCursorsAndResumeSuffixes()
    {
        using var workspace = new TemporaryDirectory();
        const string csv =
            "id,name,amount\n" +
            "1,alpha,1.25\n" +
            "2,\"bravo, incorporated\",2.50\n" +
            "3,\"charlie\nmultiline\",3.75\n" +
            "4,,4.00\n" +
            "5,echo,5.25\n";
        PackageOrigin origin = await WritePackageAsync(
            workspace,
            "replay",
            csv,
            maxDataRecords: 100);
        File.Delete(origin.SourcePath);

        string[] projection =
        [
            CsvMigrationObjectIds.Column(2),
            CsvMigrationObjectIds.Column(0),
            CsvMigrationObjectIds.Column(1),
        ];
        List<MigrationDataBatch> firstBatches;
        string firstBoundary;
        await using (CsvSnapshotPackageSession firstSession =
            await OpenAsync(origin.PackagePath, workspace.Root))
        {
            MigrationReadRequest request = ReadRequest(
                firstSession.DataSource,
                projection,
                batchSize: 2);
            firstBatches = await CollectAsync(
                firstSession.DataSource.ReadAsync(request, Cancellation));
            Assert.Equal([2, 2, 1], firstBatches.Select(item => item.Rows.Count));
            firstBoundary = Assert.IsType<string>(firstBatches[0].NextCursor);
            AssertManifestEquivalent(origin.Manifest, firstSession.Manifest);
        }

        Assert.True(File.Exists(origin.PackagePath));

        await using (CsvSnapshotPackageSession secondSession =
            await OpenAsync(origin.PackagePath, workspace.Root))
        {
            MigrationReadRequest request = ReadRequest(
                secondSession.DataSource,
                projection,
                batchSize: 2);
            List<MigrationDataBatch> secondBatches = await CollectAsync(
                secondSession.DataSource.ReadAsync(request, Cancellation));
            AssertBatchSequenceEqual(firstBatches, secondBatches);

            List<MigrationDataBatch> resumed = await CollectAsync(
                secondSession.DataSource.ReadAsync(
                    request with { ResumeCursor = firstBoundary },
                    Cancellation));
            AssertBatchSequenceEqual(firstBatches.Skip(1).ToArray(), resumed);
        }

        Assert.True(File.Exists(origin.PackagePath));
    }

    [Fact]
    public async Task ReopenPreservesSampledInferenceAndExplicitOverrideBehavior()
    {
        using var workspace = new TemporaryDirectory();
        const string csv =
            "id,value\n" +
            "001,10\n" +
            "002,20\n" +
            "late-text,30\n";
        var inferenceOptions = new CsvSchemaInferenceOptions
        {
            TableName = "sampled_orders",
            ColumnOverrides =
            [
                new CsvColumnSchemaOverride
                {
                    ColumnIndex = 0,
                    ExpectedHeader = "id",
                    LogicalType = CsvColumnLogicalType.SignedInteger,
                    Nullable = false,
                },
            ],
        };
        PackageOrigin origin = await WritePackageAsync(
            workspace,
            "sampled",
            csv,
            maxDataRecords: 2,
            inferenceOptions);
        File.Delete(origin.SourcePath);

        Assert.Equal(MigrationCoverageKind.Sample, origin.Schema.Coverage.Kind);
        CsvColumnSchema originalId = origin.Schema.Columns[0];
        Assert.Equal(CsvColumnSchemaResolution.ExplicitOverride, originalId.Resolution);
        Assert.Equal(CsvOverrideValidationStatus.SampleCompatible, originalId.OverrideValidation);

        await using (CsvSnapshotPackageSession session =
            await OpenAsync(origin.PackagePath, workspace.Root))
        {
            AssertSchemaEquivalent(origin.Schema, session.Schema);
            AssertCatalogEquivalent(origin.Catalog, session.Catalog);
            CsvColumnSchema restoredId = session.Schema.Columns[0];
            Assert.Equal(CsvColumnSchemaResolution.ExplicitOverride, restoredId.Resolution);
            Assert.Equal(CsvOverrideValidationStatus.SampleCompatible, restoredId.OverrideValidation);
            Assert.True(session.Schema.TryNormalizeScalar(0, "002", out string? canonical));
            Assert.Equal("2", canonical);
            Assert.False(session.Schema.TryNormalizeScalar(0, "late-text", out _));

            MigrationReadRequest request = ReadRequest(
                session.DataSource,
                [CsvMigrationObjectIds.Column(0), CsvMigrationObjectIds.Column(1)],
                batchSize: 2);
            await using IAsyncEnumerator<MigrationDataBatch> batches = session.DataSource
                .ReadAsync(request, Cancellation)
                .GetAsyncEnumerator(Cancellation);
            Assert.True(await batches.MoveNextAsync());
            Assert.Equal(
                ["1", "2"],
                batches.Current.Rows.Select(row => row.Values[0].CanonicalText));
            MigrationRowRejectedException rejection =
                await Assert.ThrowsAsync<MigrationRowRejectedException>(
                    async () => await batches.MoveNextAsync());
            Assert.Equal(CsvMigrationDataRules.TypeMismatch, rejection.Code);
            Assert.Equal(CsvMigrationObjectIds.Column(0), rejection.ColumnObjectId);
        }

        Assert.True(File.Exists(origin.PackagePath));
    }

    [Fact]
    public async Task OpenSessionsRemainReadableAfterPackageRemovalAndCleanOnlyTheirOwnWorkspaces()
    {
        using var workspace = new TemporaryDirectory();
        const string csv =
            "id,name\n" +
            "1,alpha\n" +
            "2,bravo\n" +
            "3,charlie\n" +
            "4,delta\n" +
            "5,echo\n";
        PackageOrigin origin = await WritePackageAsync(
            workspace,
            "detached-session",
            csv,
            maxDataRecords: 100);
        File.Delete(origin.SourcePath);
        Assert.Empty(Directory.EnumerateDirectories(workspace.Root));

        Task<CsvSnapshotPackageSession> firstOpen = OpenAsync(
                origin.PackagePath,
                workspace.Root)
            .AsTask();
        Task<CsvSnapshotPackageSession> secondOpen = OpenAsync(
                origin.PackagePath,
                workspace.Root)
            .AsTask();
        CsvSnapshotPackageSession[] sessions = await Task.WhenAll(firstOpen, secondOpen);
        CsvSnapshotPackageSession firstSession = sessions[0];
        CsvSnapshotPackageSession secondSession = sessions[1];
        bool firstDisposed = false;
        bool secondDisposed = false;
        string? movedPackagePath = null;
        try
        {
            string[] liveWorkspaces = Directory.EnumerateDirectories(workspace.Root).ToArray();
            Assert.Equal(2, liveWorkspaces.Length);

            try
            {
                File.Delete(origin.PackagePath);
            }
            catch (Exception exception) when (
                exception is IOException or UnauthorizedAccessException)
            {
                movedPackagePath = origin.PackagePath + ".removed";
                File.Move(origin.PackagePath, movedPackagePath, overwrite: false);
            }

            Assert.False(File.Exists(origin.PackagePath));

            string[] columns =
            [
                CsvMigrationObjectIds.Column(0),
                CsvMigrationObjectIds.Column(1),
            ];
            Task<List<MigrationDataBatch>> firstRead = CollectAsync(
                firstSession.DataSource.ReadAsync(
                    ReadRequest(firstSession.DataSource, columns, batchSize: 2),
                    Cancellation));
            Task<List<MigrationDataBatch>> secondRead = CollectAsync(
                secondSession.DataSource.ReadAsync(
                    ReadRequest(secondSession.DataSource, columns, batchSize: 2),
                    Cancellation));
            List<MigrationDataBatch>[] results = await Task.WhenAll(firstRead, secondRead);
            AssertBatchSequenceEqual(results[0], results[1]);
            Assert.Equal(
                ["1", "2", "3", "4", "5"],
                results[0]
                    .SelectMany(batch => batch.Rows)
                    .Select(row => row.Values[0].CanonicalText));

            await firstSession.DisposeAsync();
            firstDisposed = true;
            string remainingWorkspace = Assert.Single(
                Directory.EnumerateDirectories(workspace.Root));
            Assert.Contains(remainingWorkspace, liveWorkspaces);

            List<MigrationDataBatch> secondReplay = await CollectAsync(
                secondSession.DataSource.ReadAsync(
                    ReadRequest(secondSession.DataSource, columns, batchSize: 2),
                    Cancellation));
            AssertBatchSequenceEqual(results[1], secondReplay);

            await secondSession.DisposeAsync();
            secondDisposed = true;
            Assert.Empty(Directory.EnumerateDirectories(workspace.Root));
            Assert.False(File.Exists(origin.PackagePath));
        }
        finally
        {
            if (!firstDisposed)
                await firstSession.DisposeAsync();
            if (!secondDisposed)
                await secondSession.DisposeAsync();
            if (movedPackagePath is not null && File.Exists(movedPackagePath))
                File.Delete(movedPackagePath);
        }
    }

    [Fact]
    public async Task ExpectedManifestDigestPinsThePackageBeforeWorkspaceCreation()
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin trusted = await WritePackageAsync(
            workspace,
            "digest-trusted",
            "id,name\n1,alpha\n2,bravo\n",
            maxDataRecords: 100);
        PackageOrigin different = await WritePackageAsync(
            workspace,
            "digest-different",
            "id,name\n1,changed\n2,content\n",
            maxDataRecords: 100);
        File.Delete(trusted.SourcePath);
        File.Delete(different.SourcePath);
        Assert.NotEqual(trusted.Manifest.ManifestDigest, different.Manifest.ManifestDigest);
        Assert.Empty(Directory.EnumerateDirectories(workspace.Root));

        await using (CsvSnapshotPackageSession matching = await OpenAsync(
            trusted.PackagePath,
            workspace.Root,
            trusted.Manifest.ManifestDigest))
        {
            AssertManifestEquivalent(trusted.Manifest, matching.Manifest);
        }

        Assert.Empty(Directory.EnumerateDirectories(workspace.Root));
        CsvSnapshotPackageException mismatch =
            await Assert.ThrowsAsync<CsvSnapshotPackageException>(async () =>
                await OpenAsync(
                    different.PackagePath,
                    workspace.Root,
                    trusted.Manifest.ManifestDigest));
        Assert.Equal(CsvSnapshotPackageRules.IntegrityMismatch, mismatch.RuleId);
        Assert.Empty(Directory.EnumerateDirectories(workspace.Root));
        Assert.True(File.Exists(trusted.PackagePath));
        Assert.True(File.Exists(different.PackagePath));
    }

    [Fact]
    public async Task ExpectedManifestDigestRejectsInvalidOrNoncanonicalText()
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin = await WritePackageAsync(
            workspace,
            "digest-invalid",
            "id\n1\n2\n",
            maxDataRecords: 100);
        File.Delete(origin.SourcePath);
        string[] invalidDigests =
        [
            string.Empty,
            " ",
            new string('a', 64),
            "sha256:" + new string('A', 64),
            "sha256:" + new string('g', 64),
            "sha256:" + new string('a', 63),
        ];

        foreach (string invalidDigest in invalidDigests)
        {
            ArgumentException error = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await OpenAsync(origin.PackagePath, workspace.Root, invalidDigest));
            Assert.Equal("options", error.ParamName);
            Assert.Empty(Directory.EnumerateDirectories(workspace.Root));
        }

        Assert.True(File.Exists(origin.PackagePath));
    }

    [Fact]
    public async Task PackageWriteRejectsOversizedManifestTextBeforeCreatingArtifacts()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = Path.Combine(workspace.Root, "oversized-manifest.csv");
        string packagePath = Path.Combine(
            workspace.Root,
            "oversized-manifest" + CsvSnapshotPackage.FileExtension);
        await WriteTextAsync(sourcePath, "id\n1\n2\n");
        var readerOptions = new CsvReaderOptions
        {
            NullToken = new string(
                'n',
                (1024 * 1024) + 1),
        };

        await using (CsvSourceSnapshot snapshot = await CreateSnapshotAsync(
            sourcePath,
            workspace.Root))
        {
            CsvFormatInspection inspection = await CsvFormatInspector.InspectAsync(
                snapshot,
                readerOptions,
                new CsvInspectionOptions { DelimiterCandidates = [","] },
                Cancellation);
            CsvSourceBinding binding = await CsvSourceBinding.CreateAsync(
                snapshot,
                inspection,
                cancellationToken: Cancellation);
            CsvSchemaInferenceResult schema = await CsvSchemaInferer.InferAsync(
                binding,
                snapshot,
                maxDataRecords: 10,
                cancellationToken: Cancellation);

            InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
                async () => await CsvSnapshotPackage.WriteAsync(
                    packagePath,
                    snapshot,
                    schema,
                    TargetVersion,
                    Cancellation));

            Assert.Contains("serialization budget", error.Message, StringComparison.Ordinal);
            Assert.False(File.Exists(packagePath));
            Assert.Empty(Directory.GetFiles(workspace.Root, ".csdbcsv-*.tmp"));
        }

        Assert.Empty(Directory.EnumerateDirectories(workspace.Root));
    }

    private static CancellationToken Cancellation => TestContext.Current.CancellationToken;

    private static async ValueTask<PackageOrigin> WritePackageAsync(
        TemporaryDirectory workspace,
        string name,
        string csv,
        int maxDataRecords,
        CsvSchemaInferenceOptions? inferenceOptions = null)
    {
        string sourcePath = Path.Combine(workspace.Root, name + ".csv");
        string packagePath = Path.Combine(workspace.Root, name + CsvSnapshotPackage.FileExtension);
        await WriteTextAsync(sourcePath, csv);

        CsvSourceSnapshot snapshot = await CreateSnapshotAsync(sourcePath, workspace.Root);
        try
        {
            CsvSchemaInferenceResult schema = await InferAsync(
                snapshot,
                maxDataRecords,
                logicalSourceIdentity: "functional/" + name,
                inferenceOptions);
            MigrationCatalog catalog = schema.CreateCatalog(TargetVersion);
            CsvSnapshotPackageManifest manifest = await CsvSnapshotPackage.WriteAsync(
                packagePath,
                snapshot,
                schema,
                TargetVersion,
                Cancellation);
            return new PackageOrigin(sourcePath, packagePath, manifest, schema, catalog);
        }
        finally
        {
            await snapshot.DisposeAsync();
        }
    }

    private static async ValueTask<CsvSourceSnapshot> CreateSnapshotAsync(
        string sourcePath,
        string workspacePath) =>
        await CsvSourceSnapshot.CreateFromFileAsync(
            sourcePath,
            new CsvSourceSnapshotOptions
            {
                WorkspacePath = workspacePath,
                MaxSourceBytes = 1024 * 1024,
            },
            Cancellation);

    private static async ValueTask<CsvSchemaInferenceResult> InferAsync(
        CsvSourceSnapshot snapshot,
        int maxDataRecords,
        string logicalSourceIdentity,
        CsvSchemaInferenceOptions? inferenceOptions = null)
    {
        var readerOptions = new CsvReaderOptions();
        CsvFormatInspection inspection = await CsvFormatInspector.InspectAsync(
            snapshot,
            readerOptions,
            new CsvInspectionOptions { DelimiterCandidates = [","] },
            Cancellation);
        CsvSourceBinding binding = await CsvSourceBinding.CreateAsync(
            snapshot,
            inspection,
            logicalSourceIdentity,
            Cancellation);
        return await CsvSchemaInferer.InferAsync(
            binding,
            snapshot,
            maxDataRecords,
            inferenceOptions,
            Cancellation);
    }

    private static async ValueTask<CsvSnapshotPackageSession> OpenAsync(
        string packagePath,
        string workspacePath,
        string? expectedManifestDigest = null) =>
        await CsvSnapshotPackage.OpenAsync(
            packagePath,
            new CsvSnapshotPackageOpenOptions
            {
                WorkspacePath = workspacePath,
                MaxSourceBytes = 1024 * 1024,
                ExpectedManifestDigest = expectedManifestDigest,
            },
            Cancellation);

    private static MigrationReadRequest ReadRequest(
        CsvMigrationDataSource source,
        IReadOnlyList<string> columns,
        int batchSize) => new()
        {
            SourceObjectId = CsvMigrationObjectIds.Table,
            ColumnObjectIds = columns,
            BatchSize = batchSize,
            MaxBatchBytes = 1024 * 1024,
            MaxValueBytes = 256 * 1024,
            SnapshotToken = source.SnapshotIdentity,
        };

    private static void AssertManifestEquivalent(
        CsvSnapshotPackageManifest expected,
        CsvSnapshotPackageManifest actual)
    {
        Assert.Equal(expected.ManifestDigest, actual.ManifestDigest);
        Assert.Equal(expected.SnapshotIdentity, actual.SnapshotIdentity);
        Assert.Equal(expected.ContentDigest, actual.ContentDigest);
        Assert.Equal(expected.ContentLength, actual.ContentLength);
        Assert.Equal(expected.Source, actual.Source);
        Assert.Equal(expected.OptionsDigest, actual.OptionsDigest);
        Assert.Equal(expected.TargetCSharpDbVersion, actual.TargetCSharpDbVersion);
        Assert.Equal(expected.CatalogDigest, actual.CatalogDigest);
    }

    private static void AssertCatalogEquivalent(MigrationCatalog expected, MigrationCatalog actual) =>
        Assert.Equal(
            MigrationArtifactSerializer.SerializeCatalog(expected, writeIndented: false),
            MigrationArtifactSerializer.SerializeCatalog(actual, writeIndented: false));

    private static void AssertSchemaEquivalent(
        CsvSchemaInferenceResult expected,
        CsvSchemaInferenceResult actual)
    {
        Assert.Equal(expected.Source, actual.Source);
        Assert.Equal(expected.SnapshotIdentity, actual.SnapshotIdentity);
        Assert.Equal(expected.TableName, actual.TableName);
        Assert.Equal(expected.RecordsExamined, actual.RecordsExamined);
        Assert.Equal(expected.ProfileCharactersExamined, actual.ProfileCharactersExamined);
        Assert.Equal(expected.ProfileCharacterLimitReached, actual.ProfileCharacterLimitReached);
        Assert.Equal(expected.ReachedEndOfSource, actual.ReachedEndOfSource);
        Assert.Equal(expected.Coverage, actual.Coverage);
        Assert.Equal(expected.Columns.Count, actual.Columns.Count);
        for (int index = 0; index < expected.Columns.Count; index++)
        {
            CsvColumnSchema expectedColumn = expected.Columns[index];
            CsvColumnSchema actualColumn = actual.Columns[index];
            Assert.Equal(expectedColumn.ColumnIndex, actualColumn.ColumnIndex);
            Assert.Equal(expectedColumn.SourceName, actualColumn.SourceName);
            Assert.Equal(expectedColumn.OriginalHeader, actualColumn.OriginalHeader);
            Assert.Equal(expectedColumn.LogicalType, actualColumn.LogicalType);
            Assert.Equal(expectedColumn.SuggestedLogicalType, actualColumn.SuggestedLogicalType);
            Assert.Equal(expectedColumn.Resolution, actualColumn.Resolution);
            Assert.Equal(expectedColumn.Reason, actualColumn.Reason);
            Assert.Equal(expectedColumn.Confidence, actualColumn.Confidence);
            Assert.Equal(expectedColumn.Nullable, actualColumn.Nullable);
            Assert.Equal(expectedColumn.OverrideValidation, actualColumn.OverrideValidation);
            Assert.Equal(expectedColumn.Coverage, actualColumn.Coverage);
            Assert.Equal(expectedColumn.SubstantiveValueCount, actualColumn.SubstantiveValueCount);
            Assert.Equal(expectedColumn.NullCount, actualColumn.NullCount);
            Assert.Equal(expectedColumn.EmptyCount, actualColumn.EmptyCount);
            Assert.Equal(expectedColumn.MissingCount, actualColumn.MissingCount);
            Assert.Equal(expectedColumn.QuotedCount, actualColumn.QuotedCount);
            Assert.Equal(
                expectedColumn.NonCanonicalNumericCount,
                actualColumn.NonCanonicalNumericCount);
            Assert.Equal(expectedColumn.ObservedMaxLength, actualColumn.ObservedMaxLength);
            Assert.Equal(expectedColumn.ObservedPrecision, actualColumn.ObservedPrecision);
            Assert.Equal(expectedColumn.ObservedScale, actualColumn.ObservedScale);
            Assert.Equal(
                expectedColumn.FirstMissingDataRecordNumber,
                actualColumn.FirstMissingDataRecordNumber);
            Assert.Equal(
                expectedColumn.FirstOverrideMismatchDataRecordNumber,
                actualColumn.FirstOverrideMismatchDataRecordNumber);
        }

        Assert.Equal(
            expected.Diagnostics.Select(item =>
                (item.DiagnosticId, item.RuleId, item.Severity, item.ObjectId)),
            actual.Diagnostics.Select(item =>
                (item.DiagnosticId, item.RuleId, item.Severity, item.ObjectId)));
    }

    private static void AssertBatchSequenceEqual(
        IReadOnlyList<MigrationDataBatch> expected,
        IReadOnlyList<MigrationDataBatch> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (int index = 0; index < expected.Count; index++)
            AssertBatchEqual(expected[index], actual[index]);
    }

    private static void AssertBatchEqual(MigrationDataBatch expected, MigrationDataBatch actual)
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
            for (int valueIndex = 0; valueIndex < expectedRow.Values.Count; valueIndex++)
            {
                MigrationSourceValue expectedValue = expectedRow.Values[valueIndex];
                MigrationSourceValue actualValue = actualRow.Values[valueIndex];
                Assert.Equal(expectedValue.Kind, actualValue.Kind);
                Assert.Equal(expectedValue.CanonicalText, actualValue.CanonicalText);
                Assert.Equal(expectedValue.BinaryValue.ToArray(), actualValue.BinaryValue.ToArray());
            }
        }
    }

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> values)
    {
        var result = new List<T>();
        await foreach (T value in values.WithCancellation(Cancellation))
            result.Add(value);
        return result;
    }

    private static async ValueTask WriteTextAsync(string path, string contents) =>
        await File.WriteAllTextAsync(path, contents, new UTF8Encoding(false), Cancellation);

    private sealed record PackageOrigin(
        string SourcePath,
        string PackagePath,
        CsvSnapshotPackageManifest Manifest,
        CsvSchemaInferenceResult Schema,
        MigrationCatalog Catalog);

    private sealed class TemporaryDirectory : IDisposable
    {
        public TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-csv-package-tests-{Guid.NewGuid():N}");
            Directory.CreateDirectory(Root);
        }

        public string Root { get; }

        public void Dispose()
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
    }
}
