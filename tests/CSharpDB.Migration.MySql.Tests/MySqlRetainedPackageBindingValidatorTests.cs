using CSharpDB.Migration;
using CSharpDB.Migration.MySql;
using CSharpDB.Migration.Retained;

namespace CSharpDB.Migration.MySql.Tests;

public sealed class MySqlRetainedPackageBindingValidatorTests
{
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public void ValidProviderCatalogAndManifestBindingIsAccepted()
    {
        (
            MigrationCatalog catalog,
            RetainedMigrationPackageManifest manifest
        ) = CreateValidBinding();

        MySqlRetainedPackageBindingValidator.Validate(
            catalog,
            manifest);
    }

    [Fact]
    public void ContentDigestSubstitutionIsRejected()
    {
        (
            MigrationCatalog catalog,
            RetainedMigrationPackageManifest manifest
        ) = CreateValidBinding();
        RetainedMigrationPackageManifest substituted =
            manifest with
            {
                ContentDigest = Digest('c'),
            };

        AssertInvalid(catalog, substituted);
    }

    [Fact]
    public void PerTableSummaryAndDescriptorTamperingAreRejected()
    {
        (
            MigrationCatalog catalog,
            RetainedMigrationPackageManifest manifest
        ) = CreateValidBinding();
        RetainedMigrationPackageTableManifest table =
            Assert.Single(manifest.Tables);

        AssertInvalid(
            catalog,
            manifest with
            {
                Tables =
                [
                    table with
                    {
                        RowCount =
                            table.RowCount + 1,
                    },
                ],
            });
        AssertInvalid(
            catalog,
            manifest with
            {
                Tables =
                [
                    table with
                    {
                        SectionDigest = Digest('d'),
                    },
                ],
            });
        AssertInvalid(
            catalog,
            manifest with
            {
                Tables =
                [
                    table with
                    {
                        Descriptor =
                            table.Descriptor with
                            {
                                ColumnObjectIds =
                                    table.Descriptor
                                        .ColumnObjectIds
                                        .Reverse()
                                        .ToArray(),
                            },
                    },
                ],
            });
    }

    [Fact]
    public async Task GenericSelfConsistentDifferentRowsCannotReuseMysqlCatalog()
    {
        string workspace = CreateWorkspace();
        string packagePath =
            Path.Combine(
                workspace,
                "substituted.csdbmysql");
        (
            MigrationCatalog catalog,
            RetainedMigrationPackageManifest manifest
        ) = CreateValidBinding();
        try
        {
            RetainedMigrationPackageWriteResult result =
                await RetainedMigrationPackageWriter
                    .WriteAsync(
                        new RetainedMigrationPackageWriteRequest
                        {
                            OutputPath = packagePath,
                            Catalog = catalog,
                            SnapshotIdentity =
                                manifest.SnapshotIdentity,
                            Tables =
                            [
                                new RetainedMigrationTableWrite
                                {
                                    Descriptor =
                                        manifest.Tables[0]
                                            .Descriptor,
                                    Rows =
                                        SubstituteRows(),
                                },
                            ],
                            Options =
                                new RetainedMigrationPackageWriteOptions
                                {
                                    MaxPackageBytes =
                                        16 * 1024 * 1024,
                                },
                        },
                        Ct);
            Assert.NotEqual(
                manifest.ContentDigest,
                result.Manifest.ContentDigest);

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

            AssertInvalid(
                session.Catalog,
                session.Manifest);
        }
        finally
        {
            TryDelete(workspace);
        }
    }

    [Fact]
    public void ScalarCodecAndLogicalTypeMustRemainAligned()
    {
        (
            MigrationCatalog catalog,
            RetainedMigrationPackageManifest manifest
        ) = CreateValidBinding();
        MigrationCatalogObject tiny = catalog.Objects.Single(
            item =>
                item.Kind == MigrationObjectKind.Column &&
                item.SourceName == "TinyOne");
        MigrationCatalog mismatched = ReplaceObject(
            catalog,
            tiny with
            {
                Facets = ReplaceFacet(
                    tiny.Facets,
                    "logicalType",
                    "boolean"),
            });
        RetainedMigrationPackageManifest rebound =
            manifest with
            {
                CatalogDigest =
                    MigrationArtifactSerializer
                        .ComputeCatalogDigest(
                            mismatched),
            };

        AssertInvalid(mismatched, rebound);
    }

    private static (
        MigrationCatalog Catalog,
        RetainedMigrationPackageManifest Manifest)
        CreateValidBinding()
    {
        MySqlRetainedSourceBinding sourceBinding =
            MySqlRetainedBinding.Create(
                MySqlRetainedTestFixture
                    .CreateCaptureCatalog(
                        includeUnavailable: false),
                new MySqlRetainedCaptureOptions());
        MySqlRetainedTableBinding table =
            Assert.Single(
                sourceBinding.AvailableTables);
        var descriptor =
            new RetainedMigrationTableDescriptor
            {
                SourceObjectId =
                    table.CatalogObject.ObjectId,
                ColumnObjectIds =
                    table.Columns
                        .Select(static column =>
                            column.CatalogObject
                                .ObjectId)
                        .ToArray(),
                OrderingKeyColumnObjectIds =
                    (table.Order ??
                     throw new InvalidOperationException())
                    .Columns
                    .Select(static column =>
                        column.CatalogObject.ObjectId)
                    .ToArray(),
            };
        var summary =
            new RetainedMigrationContentSummary
            {
                DigestAlgorithm =
                    RetainedMigrationPackageContract
                        .ContentDigestAlgorithm,
                ContentDigest = Digest('b'),
                Tables =
                [
                    new RetainedMigrationContentTableSummary
                    {
                        Descriptor = descriptor,
                        RowCount = 2,
                        SectionDigest = Digest('a'),
                    },
                ],
            };
        RetainedMigrationCatalogBinding retained =
            MySqlRetainedCatalog.Create(
                sourceBinding,
                summary);
        var manifest =
            new RetainedMigrationPackageManifest
            {
                Format =
                    RetainedMigrationPackageContract.Format,
                CatalogDigest =
                    MigrationArtifactSerializer
                        .ComputeCatalogDigest(
                            retained.Catalog),
                SourceKind = MigrationSourceKind.MySql,
                SourceIdentity =
                    retained.Catalog.Source.Identity,
                SourceFingerprint =
                    retained.Catalog.Source.Fingerprint,
                SnapshotIdentity =
                    retained.SnapshotIdentity,
                ContentDigest = summary.ContentDigest,
                Tables =
                [
                    new RetainedMigrationPackageTableManifest
                    {
                        Descriptor = descriptor,
                        RowCount = 2,
                        SectionLength = 100,
                        SectionDigest = Digest('a'),
                    },
                ],
            };
        return (retained.Catalog, manifest);
    }

    private static async IAsyncEnumerable<
        MigrationDataRow> SubstituteRows()
    {
        await Task.Yield();
        yield return new MigrationDataRow
        {
            StableKey = "substituted",
            Values =
            [
                Text(
                    MigrationSourceValueKind
                        .UnsignedInteger,
                    "7"),
                Text(
                    MigrationSourceValueKind
                        .SignedInteger,
                    "2"),
                Text(
                    MigrationSourceValueKind.Decimal,
                    "9.5"),
                Text(
                    MigrationSourceValueKind
                        .FloatingPoint,
                    "0.5"),
                Text(
                    MigrationSourceValueKind.Text,
                    "different"),
                new MigrationSourceValue
                {
                    Kind =
                        MigrationSourceValueKind.Binary,
                    BinaryValue =
                        new byte[] { 9, 8, 7 },
                },
                Text(
                    MigrationSourceValueKind.Date,
                    "2026-07-25"),
                Text(
                    MigrationSourceValueKind.DateTime,
                    "2026-07-25 12:00:00"),
            ],
        };
    }

    private static MigrationSourceValue Text(
        MigrationSourceValueKind kind,
        string value) => new()
        {
            Kind = kind,
            CanonicalText = value,
        };

    private static void AssertInvalid(
        MigrationCatalog catalog,
        RetainedMigrationPackageManifest manifest)
    {
        MySqlMigrationException error =
            Assert.Throws<MySqlMigrationException>(
                () => MySqlRetainedPackageBindingValidator
                    .Validate(
                        catalog,
                        manifest));
        Assert.Equal(
            "The retained MySQL package binding is invalid.",
            error.Message);
        Assert.Null(error.InnerException);
    }

    private static IReadOnlyList<MigrationCatalogFacet>
        ReplaceFacet(
        IReadOnlyList<MigrationCatalogFacet> facets,
        string name,
        string value) =>
        facets.Select(facet =>
                string.Equals(
                    facet.Name,
                    name,
                    StringComparison.Ordinal)
                    ? facet with { Value = value }
                    : facet)
            .ToArray();

    private static MigrationCatalog ReplaceObject(
        MigrationCatalog catalog,
        MigrationCatalogObject replacement) =>
        catalog with
        {
            Objects = catalog.Objects
                .Select(item =>
                    string.Equals(
                        item.ObjectId,
                        replacement.ObjectId,
                        StringComparison.Ordinal)
                        ? replacement
                        : item)
                .ToArray(),
        };

    private static string Digest(char value) =>
        "sha256:" + new string(value, 64);

    private static string CreateWorkspace()
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            "csharpdb-mysql-binding-tests",
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
}
