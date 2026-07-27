using CSharpDB.Migration;
using CSharpDB.Migration.SqlServer;

namespace CSharpDB.Migration.SqlServer.Tests;

public sealed partial class SqlServerCatalogBuilderTests
{
    [Fact]
    public void BuildInventoriesPhysicalMetadataWithoutOverstatingCompatibility()
    {
        MigrationCatalog catalog = Build(SqlServerTestSnapshot.Create());
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById =
            catalog.Objects.ToDictionary(
                static item => item.ObjectId,
                StringComparer.Ordinal);

        MigrationCatalogObject database = Assert.Single(
            catalog.Objects,
            static item => item.Kind == MigrationObjectKind.Database);
        Assert.Equal("1", Facet(database, "sqlServerIndexedViewIndexCount"));
        Assert.Equal("1", Facet(database, "sqlServerFullTextCatalogCount"));
        Assert.Equal("1", Facet(database, "sqlServerFullTextStoplistCount"));
        Assert.Equal("1", Facet(database, "sqlServerSearchPropertyListCount"));
        Assert.Equal("1", Facet(database, "sqlServerFullTextIndexCount"));
        Assert.Equal("3", Facet(database, "sqlServerDataSpaceCount"));
        Assert.Equal("1", Facet(database, "sqlServerPartitionFunctionCount"));
        Assert.Equal("1", Facet(database, "sqlServerPartitionSchemeCount"));
        Assert.Equal("10", Facet(database, "sqlServerPhysicalPartitionCount"));
        Assert.Equal(
            "captured",
            Facet(database, "sqlServerIndexedViewIndexInventoryStatus"));
        Assert.Equal(
            "captured",
            Facet(database, "sqlServerFullTextInventoryStatus"));
        Assert.Equal(
            "captured",
            Facet(database, "sqlServerPartitionStorageInventoryStatus"));

        MigrationCatalogObject indexedViewIndex = FindObject(
            catalog,
            MigrationObjectKind.Index,
            "CUX_OrderSummary_Id");
        Assert.Equal(
            "OrderSummary",
            objectsById[indexedViewIndex.ParentObjectId!].SourceName);
        Assert.Equal(
            "sqlserver-indexed-view-index",
            Facet(indexedViewIndex, "kind"));
        Assert.Equal("true", Facet(indexedViewIndex, "sqlServerIndexedView"));
        Assert.Equal(
            ["Id"],
            MemberNames(
                catalog,
                indexedViewIndex,
                MigrationObjectReferenceRoles.Column));

        MigrationCatalogObject fullTextIndex = FindObject(
            catalog,
            MigrationObjectKind.Index,
            "$fulltext");
        Assert.Equal(
            "Orders",
            objectsById[fullTextIndex.ParentObjectId!].SourceName);
        Assert.Equal(
            "sqlserver-full-text-index",
            Facet(fullTextIndex, "kind"));
        Assert.Equal(
            "AUTO",
            Facet(
                fullTextIndex,
                "sqlServerChangeTrackingStateDescription"));
        Assert.Equal(
            ["Customer"],
            MemberNames(
                catalog,
                fullTextIndex,
                MigrationObjectReferenceRoles.Column));
        string[] fullTextDependencies = DependencyNames(catalog, fullTextIndex);
        Assert.Contains("PK_Orders", fullTextDependencies);
        Assert.Contains("MigrationSearch", fullTextDependencies);
        Assert.Contains("MigrationStoplist", fullTextDependencies);
        Assert.Contains("MigrationProperties", fullTextDependencies);
        Assert.Contains("PRIMARY", fullTextDependencies);
        Assert.Contains("Customer", fullTextDependencies);

        MigrationCatalogObject fullTextColumn = FindPhysicalObject(
            catalog,
            "full-text-index-column",
            "Customer");
        Assert.Equal(fullTextIndex.ObjectId, fullTextColumn.ParentObjectId);
        Assert.Equal("1033", Facet(fullTextColumn, "sqlServerLanguageId"));
        Assert.Equal(
            "true",
            Facet(fullTextColumn, "sqlServerStatisticalSemantics"));

        MigrationCatalogObject archiveDataSpace = FindPhysicalObject(
            catalog,
            "data-space",
            "ARCHIVE");
        Assert.Equal("true", Facet(archiveDataSpace, "sqlServerReadOnly"));

        MigrationCatalogObject partitionFunction = FindPhysicalObject(
            catalog,
            "partition-function",
            "PF_Orders_Customer");
        Assert.Equal("2", Facet(partitionFunction, "sqlServerFanout"));
        Assert.Equal(
            "true",
            Facet(partitionFunction, "sqlServerBoundaryValueOnRight"));

        MigrationCatalogObject boundary = FindPhysicalObject(
            catalog,
            "partition-boundary",
            "$boundary-1");
        Assert.Equal(partitionFunction.ObjectId, boundary.ParentObjectId);
        Assert.Equal("68", Facet(boundary, "sqlServerBoundaryValueBytes"));
        Assert.StartsWith(
            "sha256:",
            Facet(boundary, "sqlServerBoundaryValueDigest"),
            StringComparison.Ordinal);

        MigrationCatalogObject partitionScheme = FindPhysicalObject(
            catalog,
            "partition-scheme",
            "PS_Orders_Customer");
        Assert.Equal(
            "30",
            Facet(partitionScheme, "sqlServerPartitionFunctionId"));
        Assert.Contains(partitionFunction.ObjectId, partitionScheme.DependsOn);

        MigrationCatalogObject secondPartition = Assert.Single(
            catalog.Objects,
            item =>
                Facet(item, "sqlServerObjectClass") == "physical-partition" &&
                item.SourceName == "$partition-2" &&
                Facet(item, "sqlServerIndexId") == "5");
        Assert.Equal("2", Facet(secondPartition, "sqlServerPartitionNumber"));
        Assert.Equal(
            "PAGE",
            Facet(secondPartition, "sqlServerDataCompressionDescription"));
        Assert.Equal("true", Facet(secondPartition, "sqlServerXmlCompression"));
        Assert.Equal(
            "IX_Orders_Amount_Filtered",
            objectsById[secondPartition.ParentObjectId!].SourceName);
        Assert.Contains(archiveDataSpace.ObjectId, secondPartition.DependsOn);
        Assert.Contains(partitionScheme.ObjectId, secondPartition.DependsOn);

        MigrationCatalogObject heapPartition = Assert.Single(
            catalog.Objects,
            item =>
                Facet(item, "sqlServerObjectClass") == "physical-partition" &&
                Facet(item, "sqlServerObjectId") == "100" &&
                Facet(item, "sqlServerIndexId") == "0" &&
                Facet(item, "sqlServerPartitionNumber") == "1");
        Assert.Equal(
            "10",
            Facet(heapPartition, "sqlServerDefinitionDataSpaceId"));
        Assert.Contains(partitionScheme.ObjectId, heapPartition.DependsOn);

        string[] rules = catalog.Diagnostics
            .Select(static item => item.RuleId)
            .ToArray();
        Assert.Contains(
            "MIG-SQLSERVER-INDEXED-VIEW-INDEX-UNSUPPORTED-001",
            rules);
        Assert.Contains("MIG-SQLSERVER-FULLTEXT-INDEX-UNSUPPORTED-001", rules);
        Assert.Contains(
            "MIG-SQLSERVER-PHYSICAL-STORAGE-NOT-LOWERED-001",
            rules);
        Assert.Contains("MIG-SQLSERVER-PARTITIONING-UNSUPPORTED-001", rules);

        string serialized = MigrationArtifactSerializer.SerializeCatalog(catalog);
        Assert.DoesNotContain(
            SqlServerTestSnapshot.SecretPartitionBoundary,
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            SqlServerTestSnapshot.SecretPartitionBoundaryHex,
            serialized,
            StringComparison.Ordinal);
    }

    [Fact]
    public void SnapshotFingerprintBindsPhysicalMetadata()
    {
        SqlServerCatalogSnapshot baseline = SqlServerTestSnapshot.Create();
        string expected = SqlServerCatalogBuilder.ComputeSnapshotDigest(baseline);

        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                indexes: baseline.Indexes.Select(item =>
                    item.ObjectId == 5_000
                        ? item with { FillFactor = 80 }
                        : item)));
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                fullTextIndexes:
                [
                    baseline.FullTextIndexes[0] with
                    {
                        ChangeTrackingState = "M",
                        ChangeTrackingStateDescription = "MANUAL",
                    },
                ]));
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                fullTextIndexColumns:
                [
                    baseline.FullTextIndexColumns[0] with
                    {
                        LanguageId = 1031,
                    },
                ]));
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                dataSpaces: baseline.DataSpaces.Select(item =>
                    item.DataSpaceId == 11
                        ? item with { IsReadOnly = false }
                        : item)));
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                partitionRangeValues:
                [
                    baseline.PartitionRangeValues[0] with
                    {
                        ValueHex = "00",
                        ValueBytes = 1,
                    },
                ]));
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                indexPartitions: baseline.IndexPartitions.Select(item =>
                    item.ObjectId == 100 &&
                    item.IndexId == 5 &&
                    item.PartitionNumber == 2
                        ? item with
                        {
                            DataCompression = 1,
                            DataCompressionDescription = "ROW",
                        }
                         : item)));
        AssertDigestChanges(
            expected,
            Rebuild(
                baseline,
                indexPartitions: baseline.IndexPartitions.Select(item =>
                    item.ObjectId == 100 &&
                    item.IndexId == 0
                        ? item with { DefinitionDataSpaceId = 1 }
                        : item)));
    }

    [Fact]
    public void PartitionInventoryAcceptsSqlServerFloat53Parameters()
    {
        SqlServerCatalogSnapshot baseline = SqlServerTestSnapshot.Create();
        SqlServerCatalogSnapshot snapshot = Rebuild(
            baseline,
            partitionParameters:
            [
                baseline.PartitionParameters[0] with
                {
                    TypeName = "float",
                    SystemTypeName = "float",
                    MaxLength = 8,
                    Precision = 53,
                    Scale = 0,
                    Collation = null,
                },
            ],
            partitionRangeValues:
            [
                baseline.PartitionRangeValues[0] with
                {
                    BaseType = "float",
                    MaxLength = 8,
                    Precision = 53,
                    Scale = 0,
                    Collation = null,
                    ValueBytes = 8,
                    ValueHex = "000000000000F03F",
                },
            ]);

        MigrationCatalog catalog = Build(snapshot);

        MigrationCatalogObject parameter = FindPhysicalObject(
            catalog,
            "partition-parameter",
            "$parameter-1");
        Assert.Equal("53", Facet(parameter, "sqlServerPrecision"));
        Assert.Equal("float", Facet(parameter, "sqlServerSystemTypeName"));
    }

    [Fact]
    public void RestrictedVisibilityKeepsUnresolvedFullTextConfigurationUnknown()
    {
        SqlServerDatabaseMetadata restrictedDatabase =
            SqlServerTestSnapshot.Database() with
            {
                IsSysAdmin = false,
                IsDbOwner = false,
                HasControl = false,
            };
        SqlServerCatalogSnapshot snapshot = SqlServerTestSnapshot.Create(
            database: restrictedDatabase,
            searchPropertyLists: []);

        MigrationCatalog catalog = Build(snapshot);

        MigrationCatalogObject fullTextIndex = FindObject(
            catalog,
            MigrationObjectKind.Index,
            "$fulltext");
        MigrationDiagnostic diagnostic = Assert.Single(
            catalog.Diagnostics,
            item =>
                item.ObjectId == fullTextIndex.ObjectId &&
                item.RuleId ==
                    "MIG-SQLSERVER-FULLTEXT-METADATA-INCOMPLETE-001");
        Assert.Equal(MigrationCompatibilityStatus.Unknown, diagnostic.Status);
        Assert.Equal(MigrationDiagnosticSeverity.Error, diagnostic.Severity);
        Assert.False(diagnostic.CanOverride);
        Assert.DoesNotContain(
            fullTextIndex.DependsOn,
            dependency => catalog.Objects.Any(item =>
                item.ObjectId == dependency &&
                item.SourceName == "MigrationProperties"));
    }

    [Fact]
    public void PhysicalInventoryFailsClosedForInvalidReferencesAndBounds()
    {
        SqlServerCatalogSnapshot snapshot = SqlServerTestSnapshot.Create();

        Assert.Throws<SqlServerMigrationException>(() => Build(
            Rebuild(
                snapshot,
                dataSpaces: snapshot.DataSpaces.Where(
                    static item => item.DataSpaceId != 11))));
        Assert.Throws<SqlServerMigrationException>(() => Build(
            Rebuild(
                snapshot,
                fullTextIndexes:
                [
                    snapshot.FullTextIndexes[0] with
                    {
                        UniqueIndexId = 3,
                    },
                ])));
        Assert.Throws<SqlServerMigrationException>(() => Build(
            Rebuild(
                snapshot,
                partitionParameters: [])));

        SqlServerIndexPartitionMetadata[] WithHeap(
            params SqlServerIndexPartitionMetadata[] heapPartitions) =>
            snapshot.IndexPartitions
                .Where(static item => item.ObjectId != 100 || item.IndexId != 0)
                .Concat(heapPartitions)
                .ToArray();

        SqlServerIndexPartitionMetadata[] validPartitions = WithHeap(
            SqlServerTestSnapshot.IndexPartition(
                100,
                0,
                1,
                definitionDataSpaceId: 10,
                storageDataSpaceId: 1),
            SqlServerTestSnapshot.IndexPartition(
                100,
                0,
                2,
                definitionDataSpaceId: 10,
                storageDataSpaceId: 11));
        void AssertInvalidPartitions(
            IEnumerable<SqlServerIndexPartitionMetadata> partitions) =>
            Assert.Throws<SqlServerMigrationException>(() => Build(
                Rebuild(snapshot, indexPartitions: partitions)));

        AssertInvalidPartitions(validPartitions.Select(item =>
            item.ObjectId == 100 &&
            item.IndexId == 5 &&
            item.PartitionNumber == 1
                ? item with { StorageDataSpaceId = 11 }
                : item));
        AssertInvalidPartitions(WithHeap(
            SqlServerTestSnapshot.IndexPartition(
                100,
                0,
                1,
                definitionDataSpaceId: 10,
                storageDataSpaceId: 1),
            SqlServerTestSnapshot.IndexPartition(
                100,
                0,
                2,
                definitionDataSpaceId: 1,
                storageDataSpaceId: 1)));
        AssertInvalidPartitions(WithHeap(
            SqlServerTestSnapshot.IndexPartition(
                100,
                0,
                1,
                definitionDataSpaceId: 1,
                storageDataSpaceId: 1),
            SqlServerTestSnapshot.IndexPartition(
                100,
                0,
                2,
                definitionDataSpaceId: 1,
                storageDataSpaceId: 1)));
        AssertInvalidPartitions(WithHeap(
            SqlServerTestSnapshot.IndexPartition(
                100,
                0,
                1,
                definitionDataSpaceId: null,
                storageDataSpaceId: 1)));
        AssertInvalidPartitions(WithHeap(
            SqlServerTestSnapshot.IndexPartition(
                100,
                0,
                1,
                definitionDataSpaceId: 1,
                storageDataSpaceId: null)));

        Assert.Throws<SqlServerMigrationException>(() =>
            SqlServerCatalogBuilder.Build(
                snapshot,
                Request(),
                new SqlServerInspectionLimits { MaxDataSpaces = 2 },
                Ct));
        Assert.Throws<SqlServerMigrationException>(() =>
            SqlServerCatalogBuilder.Build(
                snapshot,
                Request(),
                new SqlServerInspectionLimits { MaxIndexPartitions = 8 },
                Ct));

        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        Assert.ThrowsAny<OperationCanceledException>(() =>
            SqlServerCatalogBuilder.Build(
                snapshot,
                Request(),
                SqlServerInspectionLimits.Default,
                canceled.Token));
    }

    private static MigrationCatalogObject FindPhysicalObject(
        MigrationCatalog catalog,
        string objectClass,
        string sourceName) =>
        Assert.Single(
            catalog.Objects,
            item =>
                item.Kind == MigrationObjectKind.Other &&
                item.SourceName == sourceName &&
                Facet(item, "sqlServerObjectClass") == objectClass);
}
