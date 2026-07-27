using CSharpDB.Migration;
using CSharpDB.Migration.Retained;

namespace CSharpDB.Migration.Access.Tests;

public sealed class AccessLiveQualificationTests
{
    private const string LiveFixtureEnvironmentVariable =
        "CSHARPDB_ACCESS_LIVE_FIXTURE";

    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task AceFixtureInspectsCapturesAndReplays()
    {
        string? fixture =
            Environment.GetEnvironmentVariable(
                LiveFixtureEnvironmentVariable);
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(fixture),
            $"Set {LiveFixtureEnvironmentVariable} to a trusted, unencrypted .mdb or .accdb fixture.");
        Assert.True(File.Exists(fixture));

        string workspace = Path.Combine(
            Path.GetTempPath(),
            "csharpdb-access-live-" +
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(workspace);
        string packagePath =
            Path.Combine(
                workspace,
                "fixture.csdbaccess");
        try
        {
            var inspector =
                new AccessMigrationSourceInspector(
                    fixture);
            MigrationCatalog catalog =
                await inspector.InspectAsync(
                    new MigrationInspectionRequest
                    {
                        TargetCSharpDbVersion =
                            CSharpDbCapabilityCatalogLoader
                                .CurrentTargetVersion,
                        IncludeProfile = false,
                    },
                    Ct);

            MigrationContractValidator.ValidateCatalog(
                catalog);
            Assert.Equal(
                MigrationSourceKind.Access,
                catalog.Source.Kind);
            Assert.Contains(
                catalog.Objects,
                static item =>
                    item.Kind ==
                    MigrationObjectKind.Table);

            RetainedMigrationPackageWriteResult
                capture =
                await AccessRetainedCapture.CaptureAsync(
                    fixture,
                    packagePath,
                    new AccessRetainedCaptureOptions
                    {
                        MaxPackageBytes =
                            64 * 1024 * 1024,
                    },
                    Ct);
            Assert.Contains(
                capture.Manifest.Tables,
                static table =>
                    table.RowCount > 0);

            await using
                RetainedMigrationPackageSession
                session =
                await RetainedMigrationPackageSession
                    .OpenAsync(
                        packagePath,
                        new RetainedMigrationPackageOpenOptions
                        {
                            ExpectedPackageDigest =
                                capture.PackageDigest,
                            WorkspacePath = workspace,
                            MaxPackageBytes =
                                64 * 1024 * 1024,
                        },
                        Ct);
            AccessRetainedPackageBindingValidator
                .Validate(
                    session.Catalog,
                    session.Manifest);
            long rows = 0;
            foreach (
                RetainedMigrationPackageTableManifest
                    table in session.Manifest.Tables)
            {
                await foreach (
                    MigrationDataBatch batch in
                    session.DataSource.ReadAsync(
                        new MigrationReadRequest
                        {
                            SourceObjectId =
                                table.Descriptor
                                    .SourceObjectId,
                            ColumnObjectIds =
                                table.Descriptor
                                    .ColumnObjectIds,
                            BatchSize = 100,
                            MaxBatchBytes =
                                4 * 1024 * 1024,
                            MaxValueBytes =
                                1024 * 1024,
                            SnapshotToken =
                                session.Manifest
                                    .SnapshotIdentity,
                        },
                        Ct))
                {
                    rows = checked(
                        rows +
                        batch.Rows.Count);
                }
            }
            Assert.True(rows > 0);
        }
        finally
        {
            try
            {
                if (Directory.Exists(workspace))
                {
                    Directory.Delete(
                        workspace,
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
    }
}
