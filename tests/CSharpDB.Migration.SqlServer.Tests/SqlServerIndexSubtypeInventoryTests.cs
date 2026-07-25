using CSharpDB.Migration;
using CSharpDB.Migration.SqlServer;

namespace CSharpDB.Migration.SqlServer.Tests;

public sealed partial class SqlServerCatalogBuilderTests
{
    [Fact]
    public void BuildInventoriesIndexSubtypesWithoutPublishingPrivatePaths()
    {
        MigrationCatalog catalog = Build(
            SqlServerTestSnapshot.CreateSpecializedIndexes());

        MigrationCatalogObject database = Assert.Single(
            catalog.Objects,
            static item => item.Kind == MigrationObjectKind.Database);
        Assert.Equal("4", Facet(database, "sqlServerXmlIndexCount"));
        Assert.Equal(
            "1",
            Facet(database, "sqlServerSelectiveXmlIndexPathCount"));
        Assert.Equal("1", Facet(database, "sqlServerSpatialIndexCount"));
        Assert.Equal(
            "1",
            Facet(database, "sqlServerSpatialIndexTessellationCount"));
        Assert.Equal("1", Facet(database, "sqlServerHashIndexCount"));
        Assert.Equal(
            "2",
            Facet(database, "sqlServerColumnstoreIndexCount"));
        Assert.Equal("1", Facet(database, "sqlServerJsonIndexCount"));
        Assert.Equal("2", Facet(database, "sqlServerJsonIndexPathCount"));
        Assert.Equal(
            "captured",
            Facet(database, "sqlServerIndexSubtypeInventoryStatus"));

        AssertIndex(
            catalog,
            "PXML_XmlDocuments",
            "xml",
            ["Payload"]);
        AssertIndex(
            catalog,
            "SXML_XmlDocuments_Path",
            "xml",
            ["Payload"]);
        AssertIndex(
            catalog,
            "SXI_XmlDocuments",
            "xml",
            ["Payload"]);
        AssertIndex(
            catalog,
            "SSXI_XmlDocuments_Path",
            "xml",
            ["Payload"]);
        AssertIndex(
            catalog,
            "SIX_SpatialDocuments",
            "spatial",
            ["Location"]);
        AssertIndex(
            catalog,
            "HIX_MemoryDocuments",
            "hash",
            ["LookupKey"]);
        AssertIndex(
            catalog,
            "CCI_ColumnStoreFacts",
            "clustered-columnstore",
            ["FactId", "Amount", "Segment"]);
        AssertIndex(
            catalog,
            "NCCI_ColumnStoreProjection",
            "nonclustered-columnstore",
            ["ProjectionId", "ProjectionValue"]);
        AssertIndex(
            catalog,
            "JIX_JsonDocuments",
            "json",
            ["Payload"]);

        Assert.Equal(
            4,
            ObjectsOfClass(catalog, "xml-index-config").Length);
        MigrationCatalogObject selectiveXmlPath = Assert.Single(
            ObjectsOfClass(catalog, "selective-xml-index-path"));
        Assert.Single(ObjectsOfClass(catalog, "spatial-index-config"));
        Assert.Single(
            ObjectsOfClass(catalog, "spatial-index-tessellation"));
        Assert.Single(ObjectsOfClass(catalog, "hash-index-config"));
        Assert.Equal(
            2,
            ObjectsOfClass(catalog, "columnstore-index-config").Length);
        Assert.Equal(
            5,
            ObjectsOfClass(catalog, "columnstore-index-column").Length);
        Assert.Single(ObjectsOfClass(catalog, "json-index-config"));
        MigrationCatalogObject[] jsonPaths =
            ObjectsOfClass(catalog, "json-index-path");
        Assert.Equal(2, jsonPaths.Length);

        Assert.Equal(
            checked(
                SqlServerTestSnapshot.SecretSelectiveXmlPath.Length * 2)
                .ToString(System.Globalization.CultureInfo.InvariantCulture),
            Facet(selectiveXmlPath, "sqlServerPathSourceBytes"));
        Assert.StartsWith(
            "sha256:",
            Facet(selectiveXmlPath, "sqlServerPathDigest"),
            StringComparison.Ordinal);
        Assert.All(
            jsonPaths,
            path => Assert.StartsWith(
                "sha256:",
                Facet(path, "sqlServerPathDigest"),
                StringComparison.Ordinal));

        string[] subtypeRules =
        [
            "MIG-SQLSERVER-XML-INDEX-UNSUPPORTED-001",
            "MIG-SQLSERVER-SPATIAL-INDEX-UNSUPPORTED-001",
            "MIG-SQLSERVER-HASH-INDEX-UNSUPPORTED-001",
            "MIG-SQLSERVER-COLUMNSTORE-INDEX-UNSUPPORTED-001",
            "MIG-SQLSERVER-JSON-INDEX-UNSUPPORTED-001",
        ];
        foreach (string ruleId in subtypeRules)
        {
            MigrationDiagnostic[] diagnostics = catalog.Diagnostics
                .Where(item => item.RuleId == ruleId)
                .ToArray();
            Assert.NotEmpty(diagnostics);
            Assert.All(
                diagnostics,
                diagnostic =>
                {
                    Assert.Equal(
                        MigrationDiagnosticSeverity.Error,
                        diagnostic.Severity);
                    Assert.Equal(
                        MigrationCompatibilityStatus.Unsupported,
                        diagnostic.Status);
                    Assert.False(diagnostic.CanOverride);
                });
        }

        string serialized =
            MigrationArtifactSerializer.SerializeCatalog(catalog);
        Assert.DoesNotContain(
            SqlServerTestSnapshot.SecretSelectiveXmlPath,
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            SqlServerTestSnapshot.SecretJsonIndexPath,
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "XmlPathPassword",
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "JsonPathPassword",
            serialized,
            StringComparison.Ordinal);
    }

    [Fact]
    public void IndexSubtypeInventoryIsOrderIndependentAndFingerprintBound()
    {
        SqlServerCatalogSnapshot baseline =
            SqlServerTestSnapshot.CreateSpecializedIndexes();
        SqlServerCatalogSnapshot reversed = Rebuild(
            baseline,
            indexes: baseline.Indexes.Reverse(),
            indexColumns: baseline.IndexColumns.Reverse(),
            xmlIndexes: baseline.XmlIndexes.Reverse(),
            selectiveXmlIndexPaths:
                baseline.SelectiveXmlIndexPaths.Reverse(),
            spatialIndexes: baseline.SpatialIndexes.Reverse(),
            spatialIndexTessellations:
                baseline.SpatialIndexTessellations.Reverse(),
            hashIndexes: baseline.HashIndexes.Reverse(),
            jsonIndexes: baseline.JsonIndexes.Reverse(),
            jsonIndexPaths: baseline.JsonIndexPaths.Reverse());

        Assert.Equal(
            SqlServerCatalogBuilder.ComputeSnapshotDigest(baseline),
            SqlServerCatalogBuilder.ComputeSnapshotDigest(reversed));
        Assert.Equal(
            MigrationArtifactSerializer.SerializeCatalog(Build(baseline)),
            MigrationArtifactSerializer.SerializeCatalog(Build(reversed)));

        string expected =
            SqlServerCatalogBuilder.ComputeSnapshotDigest(baseline);
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                xmlIndexes: baseline.XmlIndexes.Select(item =>
                    item.IndexId == 2
                        ? item with
                        {
                            SecondaryType = "V",
                            SecondaryTypeDescription = "VALUE",
                        }
                        : item)));
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                selectiveXmlIndexPaths:
                    baseline.SelectiveXmlIndexPaths.Select(item =>
                        item with
                        {
                            Path = item.Path.Replace(
                                "orders",
                                "Orders",
                                StringComparison.Ordinal),
                        })));
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                spatialIndexTessellations:
                    baseline.SpatialIndexTessellations.Select(item =>
                        item with { CellsPerObject = 32 })));
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                hashIndexes: baseline.HashIndexes.Select(item =>
                    item with { BucketCount = 2_048 })));
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                indexColumns: baseline.IndexColumns.Select(item =>
                    item.ObjectId == 600 &&
                    item.IndexId == 1 &&
                    item.ColumnId == 2
                        ? item with { ColumnStoreOrderOrdinal = 2 }
                        : item)));
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                jsonIndexes: baseline.JsonIndexes.Select(item =>
                    item with { OptimizeForArraySearch = false })));
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                jsonIndexPaths: baseline.JsonIndexPaths.Select(item =>
                    item.PathOrdinal == 1
                        ? item with { Path = "$.root", PathBytes = 6 }
                        : item)));
    }

    [Fact]
    public void SpatialAutoGridAcceptsBaseGridTessellationWithPositiveCellCount()
    {
        SqlServerCatalogSnapshot baseline =
            SqlServerTestSnapshot.CreateSpecializedIndexes();
        SqlServerCatalogSnapshot autoGrid = Rebuild(
            baseline,
            spatialIndexes: baseline.SpatialIndexes.Select(item =>
                item with
                {
                    TessellationScheme = "GEOMETRY_AUTO_GRID",
                }),
            spatialIndexTessellations:
                baseline.SpatialIndexTessellations.Select(item =>
                    item with
                    {
                        TessellationScheme = "GEOMETRY_GRID",
                        Level1Grid = null,
                        Level1GridDescription = null,
                        Level2Grid = null,
                        Level2GridDescription = null,
                        Level3Grid = null,
                        Level3GridDescription = null,
                        Level4Grid = null,
                        Level4GridDescription = null,
                        CellsPerObject = 8,
                    }));

        MigrationCatalog catalog = Build(autoGrid);

        MigrationCatalogObject config = Assert.Single(
            ObjectsOfClass(catalog, "spatial-index-config"));
        Assert.Equal(
            "GEOMETRY_AUTO_GRID",
            Facet(config, "sqlServerTessellationScheme"));
        MigrationCatalogObject tessellation = Assert.Single(
            ObjectsOfClass(catalog, "spatial-index-tessellation"));
        Assert.Equal(
            "GEOMETRY_GRID",
            Facet(tessellation, "sqlServerTessellationScheme"));
        Assert.Equal(
            "8",
            Facet(tessellation, "sqlServerCellsPerObject"));
        Assert.Null(Facet(tessellation, "sqlServerLevel1Grid"));
        Assert.Null(
            Facet(tessellation, "sqlServerLevel1GridDescription"));
    }

    [Fact]
    public void IndexSubtypeInventoryFailsClosedForInvalidMetadataAndLimits()
    {
        SqlServerCatalogSnapshot baseline =
            SqlServerTestSnapshot.CreateSpecializedIndexes();

        Assert.Throws<SqlServerMigrationException>(() =>
            Build(Rebuild(
                baseline,
                xmlIndexes: baseline.XmlIndexes.Select(item =>
                    item.IndexId == 1
                        ? item with { ObjectId = 999_999 }
                        : item))));
        Assert.Throws<SqlServerMigrationException>(() =>
            Build(Rebuild(
                baseline,
                xmlIndexes: baseline.XmlIndexes.Select(item =>
                    item.XmlIndexType == 3
                        ? item with { PathId = 10 }
                        : item))));
        Assert.Throws<SqlServerMigrationException>(() =>
            Build(Rebuild(
                baseline,
                xmlIndexes: baseline.XmlIndexes.Select(item =>
                    item.XmlIndexType == 3
                        ? item with
                        {
                            SecondaryType = "P",
                            SecondaryTypeDescription = "PATH",
                        }
                        : item))));
        Assert.Throws<SqlServerMigrationException>(() =>
            Build(Rebuild(
                baseline,
                spatialIndexes: baseline.SpatialIndexes.Select(item =>
                    item with { IndexId = 99 }))));
        Assert.Throws<SqlServerMigrationException>(() =>
            Build(Rebuild(
                baseline,
                spatialIndexTessellations:
                    baseline.SpatialIndexTessellations.Select(item =>
                        item with { CellsPerObject = 0 }))));
        Assert.Throws<SqlServerMigrationException>(() =>
            Build(Rebuild(
                baseline,
                spatialIndexTessellations:
                    baseline.SpatialIndexTessellations.Select(item =>
                        item with { CellsPerObject = 8_193 }))));
        Assert.Throws<SqlServerMigrationException>(() =>
            Build(Rebuild(
                baseline,
                spatialIndexes: baseline.SpatialIndexes.Select(item =>
                    item with
                    {
                        SpatialIndexType = 2,
                        SpatialIndexTypeDescription = "GEOGRAPHY",
                        TessellationScheme = "GEOGRAPHY_GRID",
                    }),
                spatialIndexTessellations:
                    baseline.SpatialIndexTessellations.Select(item =>
                        item with
                        {
                            TessellationScheme = "GEOGRAPHY_GRID",
                            BoundingBoxXMin = null,
                            BoundingBoxYMin = null,
                            BoundingBoxXMax = null,
                            BoundingBoxYMax = null,
                            CellsPerObject = 0,
                        }))));
        Assert.Throws<SqlServerMigrationException>(() =>
            Build(Rebuild(
                baseline,
                spatialIndexTessellations:
                    baseline.SpatialIndexTessellations.Select(item =>
                        item with
                        {
                            TessellationScheme = "GEOMETRY_AUTO_GRID",
                        }))));
        Assert.Throws<SqlServerMigrationException>(() =>
            Build(Rebuild(
                baseline,
                hashIndexes: baseline.HashIndexes.Select(item =>
                    item with { BucketCount = 0 }))));
        Assert.Throws<SqlServerMigrationException>(() =>
            Build(Rebuild(
                baseline,
                hashIndexes: baseline.HashIndexes.Select(item =>
                    item with { BucketCount = 1_073_741_825 }))));
        Assert.Throws<SqlServerMigrationException>(() =>
            Build(Rebuild(baseline, hashIndexes: [])));
        Assert.Throws<SqlServerMigrationException>(() =>
            Build(Rebuild(
                baseline,
                instance: baseline.Instance with
                {
                    ProductMajorVersion = 16,
                    ProductVersion = "16.0.4175.1",
                },
                database: baseline.Database with
                {
                    CompatibilityLevel = 160,
                },
                indexes: baseline.Indexes.Where(item => item.Type != 9),
                indexColumns: baseline.IndexColumns
                    .Where(item => item.ObjectId != 800)
                    .Select(item => item with
                    {
                        ColumnStoreOrderOrdinal =
                            item.ObjectId == 700 && item.ColumnId == 2
                                ? (byte?)1
                                : item.ColumnStoreOrderOrdinal,
                        DataClusteringOrdinal = null,
                    }),
                jsonIndexes: [],
                jsonIndexPaths: [])));
        Assert.Throws<SqlServerMigrationException>(() =>
            Build(Rebuild(
                baseline,
                jsonIndexPaths: baseline.JsonIndexPaths.Select(item =>
                    item.PathOrdinal == 2
                        ? item with { PathOrdinal = 1 }
                        : item))));
        Assert.Throws<SqlServerMigrationException>(() =>
            Build(Rebuild(
                baseline,
                selectiveXmlIndexPaths:
                    baseline.SelectiveXmlIndexPaths.Select(item =>
                        item with { PathBytes = item.PathBytes - 2 }))));

        Assert.Throws<SqlServerMigrationException>(() =>
            SqlServerCatalogBuilder.Build(
                baseline,
                Request(),
                new SqlServerInspectionLimits
                {
                    MaxXmlIndexes = 3,
                },
                Ct));
        Assert.Throws<SqlServerMigrationException>(() =>
            SqlServerCatalogBuilder.Build(
                baseline,
                Request(),
                new SqlServerInspectionLimits
                {
                    MaxJsonIndexPaths = 1,
                },
                Ct));
        Assert.Throws<SqlServerMigrationException>(() =>
            SqlServerCatalogBuilder.Build(
                baseline,
                Request(),
                new SqlServerInspectionLimits
                {
                    MaxIndexPathBytes =
                        SqlServerTestSnapshot.SecretSelectiveXmlPath.Length *
                        2 -
                        1,
                },
                Ct));

        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Assert.ThrowsAny<OperationCanceledException>(() =>
            SqlServerCatalogBuilder.Build(
                baseline,
                Request(),
                SqlServerInspectionLimits.Default,
                cancellation.Token));
    }

    [Fact]
    public void RestrictedVisibilityKeepsMissingIndexSubtypeMetadataUnknown()
    {
        SqlServerCatalogSnapshot baseline =
            SqlServerTestSnapshot.CreateSpecializedIndexes();
        SqlServerCatalogSnapshot restricted = Rebuild(
            baseline,
            database: baseline.Database with
            {
                IsSysAdmin = false,
                IsDbOwner = false,
                HasControl = false,
            },
            hashIndexes: []);

        MigrationCatalog catalog = Build(restricted);

        MigrationCatalogObject hashIndex = FindObject(
            catalog,
            MigrationObjectKind.Index,
            "HIX_MemoryDocuments");
        MigrationDiagnostic diagnostic = Assert.Single(
            catalog.Diagnostics,
            item =>
                item.ObjectId == hashIndex.ObjectId &&
                item.RuleId ==
                    "MIG-SQLSERVER-INDEX-SUBTYPE-METADATA-INCOMPLETE-001");
        Assert.Equal(
            MigrationDiagnosticSeverity.Error,
            diagnostic.Severity);
        Assert.Equal(
            MigrationCompatibilityStatus.Unknown,
            diagnostic.Status);
        Assert.False(diagnostic.CanOverride);
    }

    private static void AssertIndex(
        MigrationCatalog catalog,
        string sourceName,
        string expectedKind,
        string[] expectedMembers)
    {
        MigrationCatalogObject index = FindObject(
            catalog,
            MigrationObjectKind.Index,
            sourceName);
        Assert.Equal(expectedKind, Facet(index, "kind"));
        Assert.Equal(
            expectedMembers,
            MemberNames(
                catalog,
                index,
                MigrationObjectReferenceRoles.Column));
    }

    private static MigrationCatalogObject[] ObjectsOfClass(
        MigrationCatalog catalog,
        string objectClass) =>
        catalog.Objects
            .Where(item =>
                item.Kind == MigrationObjectKind.Other &&
                string.Equals(
                    Facet(item, "sqlServerObjectClass"),
                    objectClass,
                    StringComparison.Ordinal))
            .ToArray();
}
