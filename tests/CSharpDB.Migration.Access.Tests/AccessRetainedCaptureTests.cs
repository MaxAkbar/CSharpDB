using System.Runtime.CompilerServices;
using CSharpDB.Migration;
using CSharpDB.Migration.Retained;

namespace CSharpDB.Migration.Access.Tests;

public sealed class AccessRetainedCaptureTests
{
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task FakeCapturePublishesProviderNeutralReplay()
    {
        string workspace = CreateWorkspace();
        string packagePath =
            Path.Combine(
                workspace,
                "fixture.csdbaccess");
        var source = new FakeCaptureSource(
            AccessTestFixture.CreateSnapshot());
        try
        {
            RetainedMigrationPackageWriteResult
                result =
                await AccessRetainedCapture.CaptureAsync(
                    source,
                    packagePath,
                    new AccessRetainedCaptureOptions
                    {
                        MaxPackageBytes =
                            16 * 1024 * 1024,
                    },
                    Ct);

            Assert.True(source.Disposed);
            Assert.True(File.Exists(packagePath));
            RetainedMigrationPackageTableManifest
                tableManifest =
                Assert.Single(
                    result.Manifest.Tables);
            Assert.Equal(
                MigrationSourceKind.Access,
                result.Manifest.SourceKind);
            Assert.Equal(
                2,
                Assert.Single(
                    result.RowCounts).Value);

            await using
                RetainedMigrationPackageSession
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
                MigrationSourceKind.Access,
                session.Catalog.Source.Kind);
            AccessRetainedPackageBindingValidator
                .Validate(
                    session.Catalog,
                    session.Manifest);
            AccessMigrationException invalid =
                Assert.Throws<AccessMigrationException>(
                    () =>
                        AccessRetainedPackageBindingValidator
                            .Validate(
                                session.Catalog,
                                session.Manifest with
                                {
                                    SourceKind =
                                        MigrationSourceKind
                                            .Sqlite,
                                }));
            Assert.Equal(
                AccessMigrationErrorCode
                    .InvalidRetainedPackage,
                invalid.ErrorCode);
            Assert.Contains(
                session.Catalog.Diagnostics,
                static item =>
                    item.RuleId ==
                    AccessCatalogBuilder
                        .LiveQualificationRule);
            MigrationCatalogObject database =
                session.Catalog.Objects.Single(
                    static item =>
                        item.Kind ==
                        MigrationObjectKind.Database);
            Assert.Equal(
                AccessRetainedDataContract
                    .DataContract,
                Facet(
                    database,
                    "accessRetainedDataContract"));
            MigrationCatalogObject retainedTable =
                session.Catalog.Objects.Single(
                    item =>
                        item.ObjectId ==
                        tableManifest.Descriptor
                            .SourceObjectId);
            Assert.Equal(
                "true",
                Facet(
                    retainedTable,
                    AccessRetainedDataContract
                        .DataAvailableFacet));

            var rows =
                new List<MigrationDataRow>();
            await foreach (
                MigrationDataBatch batch in
                session.DataSource.ReadAsync(
                    new MigrationReadRequest
                    {
                        SourceObjectId =
                            tableManifest
                                .Descriptor
                                .SourceObjectId,
                        ColumnObjectIds =
                            tableManifest
                                .Descriptor
                                .ColumnObjectIds,
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
                rows.AddRange(batch.Rows);
            }

            Assert.Equal(2, rows.Count);
            Assert.Equal(
                "1",
                rows[0].Values[0]
                    .CanonicalText);
            Assert.Equal(
                "Ada",
                rows[0].Values[1]
                    .CanonicalText);
            Assert.StartsWith(
                "access-key:",
                rows[0].StableKey,
                StringComparison.Ordinal);
            Assert.Equal(
                "2",
                rows[1].Values[0]
                    .CanonicalText);
            Assert.Equal(
                MigrationSourceValueKind.Null,
                rows[1].Values[1].Kind);
        }
        finally
        {
            TryDelete(workspace);
        }
    }

    [Fact]
    public async Task TotalRowBoundFailsWithoutPublishing()
    {
        string workspace = CreateWorkspace();
        string packagePath =
            Path.Combine(
                workspace,
                "limited.csdbaccess");
        var source = new FakeCaptureSource(
            AccessTestFixture.CreateSnapshot());
        try
        {
            await Assert.ThrowsAsync<
                AccessRetainedCaptureLimitException>(
                () => AccessRetainedCapture
                    .CaptureAsync(
                        source,
                        packagePath,
                        new AccessRetainedCaptureOptions
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

    private static string CreateWorkspace()
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            "csharpdb-access-tests-" +
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
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    private static string? Facet(
        MigrationCatalogObject item,
        string name) =>
        item.Facets.FirstOrDefault(
            facet => string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal))?.Value;

    private sealed class FakeCaptureSource
        : IAccessRetainedCaptureSource
    {
        private readonly AccessCatalogSnapshot snapshot;

        internal FakeCaptureSource(
            AccessCatalogSnapshot snapshot)
        {
            this.snapshot = snapshot;
        }

        internal bool Disposed { get; private set; }

        public ValueTask<AccessCatalogSnapshot>
            ReadCatalogAsync(
            CancellationToken cancellationToken)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            return ValueTask.FromResult(snapshot);
        }

        public async IAsyncEnumerable<
            MigrationDataRow> ReadRowsAsync(
            AccessTableBinding table,
            AccessRetainedCaptureOptions options,
            AccessRetainedCaptureBudget budget,
            [EnumeratorCancellation]
            CancellationToken cancellationToken)
        {
            Assert.Equal(
                "People",
                table.Metadata.Name);
            foreach ((string Id, string? Name) row in
                     new[]
                     {
                         ("1", "Ada"),
                         ("2", null),
                     })
            {
                cancellationToken
                    .ThrowIfCancellationRequested();
                budget.AddRow();
                yield return new MigrationDataRow
                {
                    StableKey =
                        "access-key:" + row.Id,
                    Values =
                    [
                        new MigrationSourceValue
                        {
                            Kind =
                                MigrationSourceValueKind
                                    .SignedInteger,
                            CanonicalText = row.Id,
                        },
                        row.Name is null
                            ? new MigrationSourceValue
                            {
                                Kind =
                                    MigrationSourceValueKind
                                        .Null,
                            }
                            : new MigrationSourceValue
                            {
                                Kind =
                                    MigrationSourceValueKind
                                        .Text,
                                CanonicalText =
                                    row.Name,
                            },
                    ],
                };
                await Task.Yield();
            }
        }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }
}
