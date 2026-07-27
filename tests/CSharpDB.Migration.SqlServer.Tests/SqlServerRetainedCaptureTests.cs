using System.Runtime.CompilerServices;
using CSharpDB.Migration;
using CSharpDB.Migration.Retained;

namespace CSharpDB.Migration.SqlServer.Tests;

public sealed class SqlServerRetainedCaptureTests
{
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task FakeSourceCaptureDisposesThenReopensAndReplaysRows()
    {
        string workspace = CreateWorkspace();
        string packagePath =
            Path.Combine(
                workspace,
                "capture.csdbsqlserver");
        var source =
            new FakeCaptureSource(
                SqlServerRetainedTestFixture
                    .CreateCaptureCatalog());
        try
        {
            RetainedMigrationPackageWriteResult result =
                await SqlServerRetainedCapture
                    .CaptureAsync(
                        source,
                        packagePath,
                        new SqlServerRetainedCaptureOptions
                        {
                            MaxPackageBytes =
                                16 * 1024 * 1024,
                        },
                        Ct);

            Assert.True(source.Disposed);
            Assert.True(File.Exists(packagePath));
            Assert.Single(result.Manifest.Tables);
            Assert.Equal(
                2,
                Assert.Single(
                    result.RowCounts).Value);
            Assert.Equal(
                result.ContentSummary.ContentDigest,
                result.Manifest.SourceFingerprint);
            Assert.Equal(
                SqlServerRetainedDataContract
                    .SnapshotIdentityPrefix +
                result.ContentSummary.ContentDigest,
                result.Manifest.SnapshotIdentity);

            await using RetainedMigrationPackageSession
                session =
                await RetainedMigrationPackageSession
                    .OpenAsync(
                        packagePath,
                        new RetainedMigrationPackageOpenOptions
                        {
                            ExpectedPackageDigest =
                                result.PackageDigest,
                            WorkspacePath = workspace,
                            MaxPackageBytes =
                                16 * 1024 * 1024,
                        },
                        Ct);
            Assert.Equal(
                MigrationConsistencyKind.Snapshot,
                session.Catalog.Source
                    .Consistency.Kind);
            Assert.Equal(
                result.Manifest.SnapshotIdentity,
                session.Manifest.SnapshotIdentity);

            MigrationCatalogObject database =
                session.Catalog.Objects.Single(item =>
                    item.Kind ==
                    MigrationObjectKind.Database);
            Assert.Equal(
                SqlServerRetainedDataContract
                    .CatalogContract,
                SqlServerRetainedTestFixture
                    .Facet(
                        database,
                        "sqlServerCatalogContract"));
            Assert.Equal(
                SqlServerCatalogBuilder.CatalogContract,
                SqlServerRetainedTestFixture
                    .Facet(
                        database,
                        SqlServerRetainedCatalog
                            .AnalyzerCatalogContractFacet));
            Assert.Equal(
                SqlServerRetainedDataContract.DataContract,
                SqlServerRetainedTestFixture
                    .Facet(
                        database,
                        SqlServerRetainedCatalog
                            .DataContractFacet));
            Assert.Equal(
                result.Manifest.SnapshotIdentity,
                SqlServerRetainedTestFixture
                    .Facet(
                        database,
                        SqlServerRetainedCatalog
                            .SnapshotIdentityFacet));

            MigrationCatalogObject available =
                session.Catalog.Objects.Single(item =>
                    item.Kind == MigrationObjectKind.Table &&
                    item.SourceName == "Good");
            MigrationCatalogObject unavailable =
                session.Catalog.Objects.Single(item =>
                    item.Kind == MigrationObjectKind.Table &&
                    item.SourceName == "Heap");
            Assert.Equal(
                "true",
                SqlServerRetainedTestFixture.Facet(
                    available,
                    MigrationDataAvailabilityContract
                        .AvailableFacet));
            Assert.Equal(
                "2",
                SqlServerRetainedTestFixture.Facet(
                    available,
                    SqlServerRetainedCatalog
                        .RowCountFacet));
            Assert.Equal(
                "false",
                SqlServerRetainedTestFixture.Facet(
                    unavailable,
                    MigrationDataAvailabilityContract
                        .AvailableFacet));
            Assert.Equal(
                SqlServerRetainedAvailabilityReasons
                    .StableOrder,
                SqlServerRetainedTestFixture.Facet(
                    unavailable,
                    MigrationDataAvailabilityContract
                        .UnavailableReasonFacet));
            MigrationCatalogObject realColumn =
                session.Catalog.Objects.Single(item =>
                    item.Kind == MigrationObjectKind.Column &&
                    item.SourceName == "Rate");
            Assert.Equal(
                "32",
                SqlServerRetainedTestFixture.Facet(
                    realColumn,
                    SqlServerRetainedCatalog
                        .BinaryWidthFacet));

            Assert.DoesNotContain(
                session.Catalog.Diagnostics,
                item => item.RuleId ==
                    SqlServerRetainedCatalog
                        .InventoryPartialRule);
            Assert.DoesNotContain(
                session.Catalog.Diagnostics,
                item => item.RuleId ==
                    SqlServerRetainedCatalog
                        .LiveQualificationPendingRule);
            MigrationDiagnostic qualification =
                Assert.Single(
                    session.Catalog.Diagnostics,
                    item => item.RuleId ==
                        SqlServerRetainedCatalog
                            .RetainedQualificationRule);
            Assert.Equal(
                MigrationDiagnosticSeverity.Error,
                qualification.Severity);
            Assert.Equal(
                MigrationCompatibilityStatus.Unknown,
                qualification.Status);
            Assert.Equal(
                MigrationEvidenceLevel.Bound,
                qualification.Evidence);
            Assert.False(qualification.CanOverride);
            AssertAnalyzerDiagnosticsPreserved(
                source.Catalog,
                session.Catalog);

            string[] columnIds =
                result.Manifest.Tables[0]
                    .Descriptor.ColumnObjectIds
                    .ToArray();
            var batches =
                new List<MigrationDataBatch>();
            await foreach (
                MigrationDataBatch batch in
                session.DataSource.ReadAsync(
                    new MigrationReadRequest
                    {
                        SourceObjectId =
                            available.ObjectId,
                        ColumnObjectIds =
                            columnIds,
                        BatchSize = 10,
                        MaxBatchBytes =
                            1024 * 1024,
                        MaxValueBytes =
                            1024 * 1024,
                        SnapshotToken =
                            session.Manifest
                                .SnapshotIdentity,
                    },
                    Ct))
            {
                batches.Add(batch);
            }

            MigrationDataRow[] rows =
                batches
                    .SelectMany(static batch =>
                        batch.Rows)
                    .ToArray();
            Assert.Equal(2, rows.Length);
            Assert.Equal("id:1", rows[0].StableKey);
            Assert.Equal(
                "1",
                rows[0].Values[0].CanonicalText);
            Assert.Equal(
                "1234567890123456789012345678901234.5678",
                rows[0].Values[1].CanonicalText);
            Assert.Equal(
                "snow-1",
                rows[0].Values[2].CanonicalText);
            Assert.Equal(
                "1.25",
                rows[0].Values[3].CanonicalText);
            Assert.Equal("id:2", rows[1].StableKey);
        }
        finally
        {
            TryDelete(workspace);
        }
    }

    [Fact]
    public async Task TotalRowBoundFailsWithTypedLimitAndDisposesSource()
    {
        string workspace = CreateWorkspace();
        string packagePath =
            Path.Combine(
                workspace,
                "limited.csdbsqlserver");
        var source =
            new FakeCaptureSource(
                SqlServerRetainedTestFixture
                    .CreateCaptureCatalog(
                        includeHeap: false));
        try
        {
            await Assert.ThrowsAsync<
                SqlServerRetainedCaptureLimitException>(
                () => SqlServerRetainedCapture
                    .CaptureAsync(
                        source,
                        packagePath,
                        new SqlServerRetainedCaptureOptions
                        {
                            MaxRowsTotal = 1,
                        },
                        Ct)
                    .AsTask());

            Assert.True(source.Disposed);
            Assert.False(File.Exists(packagePath));
        }
        finally
        {
            TryDelete(workspace);
        }
    }

    [Fact]
    public async Task ProviderCleanupFailuresDoNotAmbiguatePublishedCapture()
    {
        string workspace = CreateWorkspace();
        string packagePath =
            Path.Combine(
                workspace,
                "cleanup.csdbsqlserver");
        var source =
            new FakeCaptureSource(
                SqlServerRetainedTestFixture
                    .CreateCaptureCatalog(
                        includeHeap: false),
                failProviderCleanup: true);
        try
        {
            RetainedMigrationPackageWriteResult result =
                await SqlServerRetainedCapture
                    .CaptureAsync(
                        source,
                        packagePath,
                        new SqlServerRetainedCaptureOptions
                        {
                            MaxPackageBytes =
                                16 * 1024 * 1024,
                        },
                        Ct);

            Assert.NotNull(result);
            Assert.True(File.Exists(packagePath));
            Assert.True(source.Disposed);
            Assert.Equal(3, source.CleanupSteps);
        }
        finally
        {
            TryDelete(workspace);
        }
    }

    [Fact]
    public async Task ProviderCleanupFailuresDoNotMaskPrimaryCaptureFailure()
    {
        string workspace = CreateWorkspace();
        string packagePath =
            Path.Combine(
                workspace,
                "primary-failure.csdbsqlserver");
        var primary =
            new SqlServerMigrationException(
                "simulated primary capture failure");
        var source =
            new FakeCaptureSource(
                SqlServerRetainedTestFixture
                    .CreateCaptureCatalog(
                        includeHeap: false),
                failProviderCleanup: true,
                catalogFailure: primary);
        try
        {
            SqlServerMigrationException actual =
                await Assert.ThrowsAsync<
                    SqlServerMigrationException>(
                    () => SqlServerRetainedCapture
                        .CaptureAsync(
                            source,
                            packagePath,
                            new SqlServerRetainedCaptureOptions(),
                            Ct)
                        .AsTask());

            Assert.Same(primary, actual);
            Assert.True(source.Disposed);
            Assert.Equal(3, source.CleanupSteps);
            Assert.False(File.Exists(packagePath));
        }
        finally
        {
            TryDelete(workspace);
        }
    }

    private static void AssertAnalyzerDiagnosticsPreserved(
        MigrationCatalog analyzer,
        MigrationCatalog retained)
    {
        string[] expected =
            analyzer.Diagnostics
                .Where(item =>
                    item.RuleId !=
                    SqlServerRetainedCatalog
                        .InventoryPartialRule &&
                    item.RuleId !=
                    SqlServerRetainedCatalog
                        .LiveQualificationPendingRule)
                .Select(static item =>
                    item.DiagnosticId)
                .OrderBy(
                    static item => item,
                    StringComparer.Ordinal)
                .ToArray();
        string[] actual =
            retained.Diagnostics
                .Where(item =>
                    item.RuleId !=
                    SqlServerRetainedCatalog
                        .RetainedQualificationRule)
                .Select(static item =>
                    item.DiagnosticId)
                .OrderBy(
                    static item => item,
                    StringComparer.Ordinal)
                .ToArray();
        Assert.Equal(expected, actual);
    }

    private static string CreateWorkspace()
    {
        string path =
            Path.Combine(
                Path.GetTempPath(),
                "csharpdb-sqlserver-retained-tests",
                Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // Test cleanup is best effort.
        }
    }

    private sealed class FakeCaptureSource :
        ISqlServerRetainedCaptureSource
    {
        private readonly Exception? catalogFailure;
        private readonly bool failProviderCleanup;
        private int cleanupSteps;
        private int disposed;

        internal FakeCaptureSource(
            MigrationCatalog catalog,
            bool failProviderCleanup = false,
            Exception? catalogFailure = null)
        {
            Catalog = catalog;
            this.failProviderCleanup =
                failProviderCleanup;
            this.catalogFailure =
                catalogFailure;
        }

        internal MigrationCatalog Catalog { get; }

        internal bool Disposed =>
            Volatile.Read(ref disposed) != 0;

        internal int CleanupSteps =>
            Volatile.Read(ref cleanupSteps);

        public ValueTask<MigrationCatalog>
            ReadCatalogAsync(
            CancellationToken cancellationToken)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            ObjectDisposedException.ThrowIf(
                Disposed,
                this);
            return catalogFailure is null
                ? ValueTask.FromResult(Catalog)
                : ValueTask.FromException<
                    MigrationCatalog>(
                    catalogFailure);
        }

        public async IAsyncEnumerable<
            MigrationDataRow> ReadRowsAsync(
            SqlServerRetainedTableBinding table,
            SqlServerRetainedCaptureOptions options,
            SqlServerRetainedCaptureBudget budget,
            [EnumeratorCancellation]
            CancellationToken cancellationToken)
        {
            ObjectDisposedException.ThrowIf(
                Disposed,
                this);
            Assert.Equal("Good", table.CatalogObject.SourceName);
            for (int row = 1; row <= 2; row++)
            {
                cancellationToken
                    .ThrowIfCancellationRequested();
                budget.AddRow();
                await Task.Yield();
                yield return new MigrationDataRow
                {
                    StableKey = $"id:{row}",
                    Values =
                    [
                        Text(
                            MigrationSourceValueKind
                                .SignedInteger,
                            row.ToString(
                                System.Globalization
                                    .CultureInfo
                                    .InvariantCulture)),
                        Text(
                            MigrationSourceValueKind
                                .Decimal,
                            row == 1
                                ? "1234567890123456789012345678901234.5678"
                                : "2.5"),
                        Text(
                            MigrationSourceValueKind.Text,
                            $"snow-{row}"),
                        Text(
                            MigrationSourceValueKind
                                .FloatingPoint,
                            row == 1
                                ? "1.25"
                                : "2.5"),
                    ],
                };
            }
        }

        public ValueTask DisposeAsync()
        {
            Interlocked.Exchange(
                ref disposed,
                1);
            return failProviderCleanup
                ? SqlServerRetainedCleanup.DisposeAsync(
                    FailCleanupStep,
                    FailCleanupStep,
                    FailCleanupStep)
                : ValueTask.CompletedTask;
        }

        private ValueTask FailCleanupStep()
        {
            Interlocked.Increment(
                ref cleanupSteps);
            return ValueTask.FromException(
                new InvalidOperationException(
                    "simulated provider cleanup failure"));
        }

        private static MigrationSourceValue Text(
            MigrationSourceValueKind kind,
            string value) => new()
            {
                Kind = kind,
                CanonicalText = value,
            };
    }
}
