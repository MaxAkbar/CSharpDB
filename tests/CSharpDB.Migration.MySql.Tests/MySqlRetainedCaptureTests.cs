using System.Runtime.CompilerServices;
using CSharpDB.Migration;
using CSharpDB.Migration.MySql;
using CSharpDB.Migration.Retained;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.MySql.Tests;

public sealed class MySqlRetainedCaptureTests
{
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task SelectOnlyFakeCaptureRechecksCatalogAndReplaysRows()
    {
        string workspace = CreateWorkspace();
        string packagePath =
            Path.Combine(workspace, "capture.csdbmysql");
        var source = new FakeCaptureSource(
            MySqlRetainedTestFixture
                .CreateCaptureCatalog());
        try
        {
            RetainedMigrationPackageWriteResult result =
                await MySqlRetainedCapture.CaptureAsync(
                    source,
                    packagePath,
                    new MySqlRetainedCaptureOptions
                    {
                        MaxPackageBytes =
                            16 * 1024 * 1024,
                    },
                    Ct);

            Assert.True(source.Disposed);
            Assert.Equal(2, source.CatalogReads);
            Assert.Equal(1, source.RowReadCalls);
            Assert.True(File.Exists(packagePath));
            Assert.Single(result.Manifest.Tables);
            Assert.Equal(
                2,
                Assert.Single(result.RowCounts).Value);
            Assert.Equal(
                MySqlRetainedDataContract
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
            MigrationCatalogObject database =
                session.Catalog.Objects.Single(item =>
                    item.Kind ==
                    MigrationObjectKind.Database);
            Assert.Equal(
                MySqlRetainedDataContract.CatalogContract,
                Facet(
                    database,
                    "mysqlCatalogContract"));
            Assert.Equal(
                MySqlCatalogBuilder.CatalogContract,
                Facet(
                    database,
                    MySqlRetainedCatalog
                        .AnalyzerCatalogContractFacet));
            Assert.Equal(
                MySqlRetainedDataContract.DataContract,
                Facet(
                    database,
                    MySqlRetainedCatalog
                        .DataContractFacet));
            Assert.Equal(
                MySqlRetainedCatalog.MetadataScope,
                Facet(
                    database,
                    MySqlRetainedCatalog
                        .MetadataScopeFacet));
            Assert.Equal(
                "true",
                Facet(
                    database,
                    MySqlRetainedCatalog
                        .DirectSchemaSelectProvenFacet));
            Assert.Equal(
                result.Manifest.SnapshotIdentity,
                Facet(
                    database,
                    MySqlRetainedCatalog
                        .SnapshotIdentityFacet));
            Assert.Equal(
                MigrationConsistencyKind.Snapshot,
                session.Catalog.Source
                    .Consistency.Kind);

            MigrationCatalogObject available =
                session.Catalog.Objects.Single(item =>
                    item.Kind ==
                        MigrationObjectKind.Table &&
                    item.SourceName == "Good");
            MigrationCatalogObject unavailable =
                session.Catalog.Objects.Single(item =>
                    item.Kind ==
                        MigrationObjectKind.Table &&
                    item.SourceName == "NoKey");
            Assert.Equal(
                "true",
                Facet(
                    available,
                    MigrationDataAvailabilityContract
                        .AvailableFacet));
            Assert.Equal(
                "2",
                Facet(
                    available,
                    MySqlRetainedCatalog.RowCountFacet));
            Assert.Equal(
                MySqlRetainedDataContract.RowOrderContract,
                Facet(
                    available,
                    MySqlRetainedCatalog
                        .RowOrderContractFacet));
            Assert.Equal(
                "false",
                Facet(
                    unavailable,
                    MigrationDataAvailabilityContract
                        .AvailableFacet));
            Assert.Equal(
                MySqlRetainedAvailabilityReasons
                    .StableOrder,
                Facet(
                    unavailable,
                    MigrationDataAvailabilityContract
                        .UnavailableReasonFacet));

            Assert.DoesNotContain(
                session.Catalog.Diagnostics,
                static item =>
                    item.RuleId ==
                    MySqlRetainedCatalog.InventoryPartialRule);
            Assert.DoesNotContain(
                session.Catalog.Diagnostics,
                static item =>
                    item.RuleId ==
                    MySqlRetainedCatalog
                        .MetadataCompletenessRule);
            Assert.DoesNotContain(
                session.Catalog.Diagnostics,
                static item =>
                    item.RuleId ==
                    MySqlRetainedCatalog
                        .LiveQualificationPendingRule);
            AssertWarning(
                session.Catalog,
                MySqlRetainedCatalog.RetainedScopeRule);
            AssertWarning(
                session.Catalog,
                MySqlRetainedCatalog
                    .RetainedQualificationRule);
            Assert.Contains(
                session.Catalog.Diagnostics,
                static item =>
                    item.RuleId ==
                    "MIG-MYSQL-TINYINT-BOOLEAN-SEMANTICS-001");

            string[] columnIds =
                result.Manifest.Tables[0]
                    .Descriptor.ColumnObjectIds
                    .ToArray();
            var rows = new List<MigrationDataRow>();
            await foreach (
                MigrationDataBatch batch in
                session.DataSource.ReadAsync(
                    new MigrationReadRequest
                    {
                        SourceObjectId =
                            available.ObjectId,
                        ColumnObjectIds = columnIds,
                        BatchSize = 10,
                        MaxBatchBytes = 1024 * 1024,
                        MaxValueBytes = 1024 * 1024,
                        SnapshotToken =
                            session.Manifest
                                .SnapshotIdentity,
                    },
                    Ct))
            {
                rows.AddRange(batch.Rows);
            }

            Assert.Equal(2, rows.Count);
            Assert.Equal(
                ulong.MaxValue.ToString(
                    System.Globalization
                        .CultureInfo.InvariantCulture),
                rows[0].Values[0].CanonicalText);
            Assert.Equal(
                MigrationSourceValueKind.SignedInteger,
                rows[0].Values[1].Kind);
            Assert.Equal(
                "0.10000000149011612",
                rows[0].Values[3].CanonicalText);
            Assert.Equal(
                "2026-07-25 14:03:02.123456",
                rows[0].Values[7].CanonicalText);

            MigrationCatalogObject tinyOne =
                session.Catalog.Objects.Single(item =>
                    item.ObjectId == columnIds[1]);
            Assert.Equal(
                "signedInteger",
                Facet(tinyOne, "logicalType"));
            MigrationTypeMapping tinyOneMapping =
                new MigrationPlanner()
                    .CreatePlan(session.Catalog)
                    .Objects.Single(item =>
                        item.SourceObjectId ==
                            tinyOne.ObjectId)
                    .TypeMappings.Single();
            Assert.Equal(
                DbType.Integer,
                tinyOneMapping.TargetType);
            DbValue converted =
                MigrationValueConverter.Convert(
                    rows[1].Values[1],
                    tinyOne,
                    tinyOneMapping,
                    rowOrdinal: 1);
            Assert.Equal(DbType.Integer, converted.Type);
            Assert.Equal(2, converted.AsInteger);
        }
        finally
        {
            TryDelete(workspace);
        }
    }

    [Fact]
    public async Task CatalogChangeAfterRowsFailsBeforePublication()
    {
        string workspace = CreateWorkspace();
        string packagePath =
            Path.Combine(workspace, "changed.csdbmysql");
        MigrationCatalog initial =
            MySqlRetainedTestFixture
                .CreateCaptureCatalog(
                    includeUnavailable: false);
        MigrationCatalog changed = initial with
        {
            Source = initial.Source with
            {
                ProviderVersion = "changed",
            },
        };
        var source = new FakeCaptureSource(
            initial,
            changed);
        try
        {
            MySqlMigrationException error =
                await Assert.ThrowsAsync<
                    MySqlMigrationException>(
                    () => MySqlRetainedCapture
                        .CaptureAsync(
                            source,
                            packagePath,
                            new MySqlRetainedCaptureOptions(),
                            Ct)
                        .AsTask());

            Assert.Equal(2, source.CatalogReads);
            Assert.Equal(1, source.RowReadCalls);
            Assert.True(source.Disposed);
            Assert.False(File.Exists(packagePath));
            Assert.Equal(
                "The MySQL catalog changed during retained capture.",
                error.Message);
            Assert.Null(error.InnerException);
        }
        finally
        {
            TryDelete(workspace);
        }
    }

    [Fact]
    public async Task MissingDirectSelectFailsBeforeRowsOrOutput()
    {
        string workspace = CreateWorkspace();
        string packagePath =
            Path.Combine(
                workspace,
                "unproven.csdbmysql");
        var source = new FakeCaptureSource(
            MySqlRetainedTestFixture
                .CreateCaptureCatalog(
                    includeUnavailable: false,
                    MySqlMetadataVisibilityProof
                        .Unproven));
        try
        {
            await Assert.ThrowsAsync<
                MySqlMigrationException>(
                () => MySqlRetainedCapture
                    .CaptureAsync(
                        source,
                        packagePath,
                        new MySqlRetainedCaptureOptions(),
                        Ct)
                    .AsTask());

            Assert.Equal(1, source.CatalogReads);
            Assert.Equal(0, source.RowReadCalls);
            Assert.True(source.Disposed);
            Assert.False(File.Exists(packagePath));
        }
        finally
        {
            TryDelete(workspace);
        }
    }

    [Fact]
    public async Task TotalRowBoundFailsTypedAndDoesNotPublish()
    {
        string workspace = CreateWorkspace();
        string packagePath =
            Path.Combine(
                workspace,
                "limited.csdbmysql");
        var source = new FakeCaptureSource(
            MySqlRetainedTestFixture
                .CreateCaptureCatalog(
                    includeUnavailable: false));
        try
        {
            await Assert.ThrowsAsync<
                MySqlRetainedCaptureLimitException>(
                () => MySqlRetainedCapture
                    .CaptureAsync(
                        source,
                        packagePath,
                        new MySqlRetainedCaptureOptions
                        {
                            MaxRowsTotal = 1,
                        },
                        Ct)
                    .AsTask());

            Assert.Equal(1, source.CatalogReads);
            Assert.True(source.Disposed);
            Assert.False(File.Exists(packagePath));
        }
        finally
        {
            TryDelete(workspace);
        }
    }

    [Fact]
    public async Task CleanupAttemptsEveryStepAndSuppressesProviderFailures()
    {
        int steps = 0;
        ValueTask Fail()
        {
            Interlocked.Increment(ref steps);
            return ValueTask.FromException(
                new InvalidOperationException(
                    "provider detail"));
        }

        await MySqlRetainedCleanup.DisposeAsync(
            Fail,
            Fail,
            Fail);

        Assert.Equal(3, steps);
    }

    private static void AssertWarning(
        MigrationCatalog catalog,
        string rule)
    {
        MigrationDiagnostic diagnostic =
            Assert.Single(
                catalog.Diagnostics,
                item => item.RuleId == rule);
        Assert.Equal(
            MigrationDiagnosticSeverity.Warning,
            diagnostic.Severity);
        Assert.Equal(
            MigrationCompatibilityStatus.Conditional,
            diagnostic.Status);
        Assert.Equal(
            MigrationEvidenceLevel.Bound,
            diagnostic.Evidence);
        Assert.False(diagnostic.CanOverride);
    }

    private static string? Facet(
        MigrationCatalogObject item,
        string name) =>
        item.Facets.SingleOrDefault(facet =>
            string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal))?.Value;

    private static string CreateWorkspace()
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            "csharpdb-mysql-retained-tests",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(
                    path,
                    recursive: true);
            }
        }
        catch
        {
            // Test cleanup is best effort.
        }
    }

    private sealed class FakeCaptureSource :
        IMySqlRetainedCaptureSource
    {
        private readonly MigrationCatalog initial;
        private readonly MigrationCatalog final;
        private int catalogReads;
        private int rowReadCalls;
        private int disposed;

        internal FakeCaptureSource(
            MigrationCatalog initial,
            MigrationCatalog? final = null)
        {
            this.initial = initial;
            this.final = final ?? initial;
        }

        internal int CatalogReads =>
            Volatile.Read(ref catalogReads);

        internal int RowReadCalls =>
            Volatile.Read(ref rowReadCalls);

        internal bool Disposed =>
            Volatile.Read(ref disposed) != 0;

        public ValueTask<MigrationCatalog>
            ReadCatalogAsync(
            CancellationToken cancellationToken)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            ObjectDisposedException.ThrowIf(
                Disposed,
                this);
            int read =
                Interlocked.Increment(
                    ref catalogReads);
            return read switch
            {
                1 => ValueTask.FromResult(initial),
                2 => ValueTask.FromResult(final),
                _ => ValueTask.FromException<
                    MigrationCatalog>(
                    new InvalidOperationException(
                        "unexpected catalog read")),
            };
        }

        public async IAsyncEnumerable<
            MigrationDataRow> ReadRowsAsync(
            MySqlRetainedTableBinding table,
            MySqlRetainedCaptureOptions options,
            MySqlRetainedCaptureBudget budget,
            [EnumeratorCancellation]
            CancellationToken cancellationToken)
        {
            ObjectDisposedException.ThrowIf(
                Disposed,
                this);
            Interlocked.Increment(ref rowReadCalls);
            Assert.Equal(
                "Good",
                table.CatalogObject.SourceName);
            for (int row = 1;
                 row <= 2;
                 row++)
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
                                .UnsignedInteger,
                            row == 1
                                ? ulong.MaxValue.ToString(
                                    System.Globalization
                                        .CultureInfo
                                        .InvariantCulture)
                                : "2"),
                        Text(
                            MigrationSourceValueKind
                                .SignedInteger,
                            row.ToString(
                                System.Globalization
                                    .CultureInfo
                                    .InvariantCulture)),
                        Text(
                            MigrationSourceValueKind.Decimal,
                            row == 1
                                ? "12345678901234567890123456789012345.123456789012345678901234567890"
                                : "2.5"),
                        Text(
                            MigrationSourceValueKind
                                .FloatingPoint,
                            row == 1
                                ? "0.10000000149011612"
                                : "2.5"),
                        Text(
                            MigrationSourceValueKind.Text,
                            $"snow-{row}"),
                        new MigrationSourceValue
                        {
                            Kind =
                                MigrationSourceValueKind.Binary,
                            BinaryValue =
                                new byte[] { 0, (byte)row },
                        },
                        Text(
                            MigrationSourceValueKind.Date,
                            "2026-07-25"),
                        Text(
                            MigrationSourceValueKind.DateTime,
                            row == 1
                                ? "2026-07-25 14:03:02.123456"
                                : "2026-07-25 14:03:03"),
                    ],
                };
            }
        }

        public ValueTask DisposeAsync()
        {
            Interlocked.Exchange(ref disposed, 1);
            return ValueTask.CompletedTask;
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
