using CSharpDB.Migration;

namespace CSharpDB.Migration.SqlServer;

internal static partial class SqlServerCatalogBuilder
{
    private const string IndexSubtypeMetadataIncompleteRule =
        "MIG-SQLSERVER-INDEX-SUBTYPE-METADATA-INCOMPLETE-001";

    private static int IndexSubtypeObjectCapacity(
        SqlServerCatalogSnapshot snapshot) =>
        checked(
            snapshot.XmlIndexes.Count +
            snapshot.SelectiveXmlIndexPaths.Count +
            snapshot.SpatialIndexes.Count +
            snapshot.SpatialIndexTessellations.Count +
            snapshot.HashIndexes.Count +
            snapshot.JsonIndexes.Count +
            snapshot.JsonIndexPaths.Count +
            snapshot.Indexes.Count(static item => item.Type is 5 or 6) +
            CountColumnstoreIndexColumns(snapshot));

    private static int CountColumnstoreIndexes(SqlServerCatalogSnapshot snapshot) =>
        snapshot.Indexes.Count(static item => item.Type is 5 or 6);

    private static int CountColumnstoreIndexColumns(
        SqlServerCatalogSnapshot snapshot)
    {
        HashSet<(int ObjectId, int IndexId)> columnstoreIndexes = snapshot.Indexes
            .Where(static item => item.Type is 5 or 6)
            .Select(static item => (item.ObjectId, item.IndexId))
            .ToHashSet();
        return snapshot.IndexColumns.Count(item =>
            columnstoreIndexes.Contains((item.ObjectId, item.IndexId)));
    }

    private static void AddIndexSubtypeObjects(
        SqlServerCatalogSnapshot snapshot,
        IReadOnlyDictionary<int, PhysicalRelation> relations,
        IReadOnlyDictionary<(int ObjectId, int ColumnId), PhysicalColumn> columns,
        IReadOnlyDictionary<(int ObjectId, int IndexId), string>
            nativeIndexObjectIds,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics,
        CancellationToken cancellationToken)
    {
        Dictionary<(int ObjectId, int IndexId), SqlServerXmlIndexMetadata>
            xmlIndexes = snapshot.XmlIndexes.ToDictionary(
                static item => (item.ObjectId, item.IndexId));
        Dictionary<(int ObjectId, int IndexId),
            SqlServerSelectiveXmlIndexPathMetadata[]> xmlPaths =
            snapshot.SelectiveXmlIndexPaths
                .GroupBy(static item => (item.ObjectId, item.IndexId))
                .ToDictionary(
                    static group => group.Key,
                    static group => group
                        .OrderBy(static item => item.PathId)
                        .ToArray());
        Dictionary<(int ObjectId, int IndexId), SqlServerSpatialIndexMetadata>
            spatialIndexes = snapshot.SpatialIndexes.ToDictionary(
                static item => (item.ObjectId, item.IndexId));
        Dictionary<(int ObjectId, int IndexId),
            SqlServerSpatialIndexTessellationMetadata> spatialTessellations =
            snapshot.SpatialIndexTessellations.ToDictionary(
                static item => (item.ObjectId, item.IndexId));
        Dictionary<(int ObjectId, int IndexId), SqlServerHashIndexMetadata>
            hashIndexes = snapshot.HashIndexes.ToDictionary(
                static item => (item.ObjectId, item.IndexId));
        Dictionary<(int ObjectId, int IndexId), SqlServerJsonIndexMetadata>
            jsonIndexes = snapshot.JsonIndexes.ToDictionary(
                static item => (item.ObjectId, item.IndexId));
        Dictionary<(int ObjectId, int IndexId), SqlServerJsonIndexPathMetadata[]>
            jsonPaths = snapshot.JsonIndexPaths
                .GroupBy(static item => (item.ObjectId, item.IndexId))
                .ToDictionary(
                    static group => group.Key,
                    static group => group
                        .OrderBy(static item => item.PathOrdinal)
                        .ToArray());
        Dictionary<(int ObjectId, int IndexId), SqlServerIndexColumnMetadata[]>
            indexColumns = snapshot.IndexColumns
                .GroupBy(static item => (item.ObjectId, item.IndexId))
                .ToDictionary(
                    static group => group.Key,
                    static group => group
                        .OrderBy(static item => item.IndexColumnId)
                        .ToArray());

        foreach (SqlServerIndexMetadata index in snapshot.Indexes
                     .Where(static item => item.Type is 3 or 4 or 5 or 6 or 7 or 9)
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.IndexId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            (int ObjectId, int IndexId) key = (index.ObjectId, index.IndexId);
            PhysicalRelation relation = relations[index.ObjectId];
            string indexObjectId = nativeIndexObjectIds[key];
            indexColumns.TryGetValue(key, out SqlServerIndexColumnMetadata[]? ownedColumns);
            ownedColumns ??= [];

            bool metadataComplete;
            switch (index.Type)
            {
                case 3:
                    xmlIndexes.TryGetValue(
                        key,
                        out SqlServerXmlIndexMetadata? xmlIndex);
                    metadataComplete = HasCompleteXmlIndexMetadata(
                        key,
                        xmlIndex,
                        xmlIndexes,
                        xmlPaths);
                    if (xmlIndex is not null)
                    {
                        xmlPaths.TryGetValue(
                            key,
                            out SqlServerSelectiveXmlIndexPathMetadata[]? ownedPaths);
                        ownedPaths ??= [];
                        AddXmlIndexObjects(
                            index,
                            xmlIndex,
                            ownedPaths,
                            relation,
                            columns,
                            ownedColumns,
                            nativeIndexObjectIds,
                            indexObjectId,
                            objects);
                    }
                    AddSubtypeDiagnostic(
                        indexObjectId,
                        "MIG-SQLSERVER-XML-INDEX-UNSUPPORTED-001",
                        "The SQL Server XML index is not target-compatible.",
                        "Primary, secondary, selective, and promoted-path XML index semantics have no advertised CSharpDB lowering contract.",
                        diagnostics);
                    break;

                case 4:
                    spatialIndexes.TryGetValue(
                        key,
                        out SqlServerSpatialIndexMetadata? spatialIndex);
                    spatialTessellations.TryGetValue(
                        key,
                        out SqlServerSpatialIndexTessellationMetadata?
                            spatialTessellation);
                    metadataComplete =
                        spatialIndex is not null &&
                        spatialTessellation is not null;
                    if (spatialIndex is not null)
                    {
                        AddSpatialIndexObjects(
                            index,
                            spatialIndex,
                            spatialTessellation,
                            relation,
                            columns,
                            ownedColumns,
                            indexObjectId,
                            objects);
                    }
                    AddSubtypeDiagnostic(
                        indexObjectId,
                        "MIG-SQLSERVER-SPATIAL-INDEX-UNSUPPORTED-001",
                        "The SQL Server spatial index is not target-compatible.",
                        "Spatial type, tessellation, grid, and bounding-box semantics have no advertised CSharpDB lowering contract.",
                        diagnostics);
                    break;

                case 5:
                case 6:
                    metadataComplete = HasCompleteColumnstoreMetadata(
                        snapshot.Instance.ProductMajorVersion,
                        ownedColumns);
                    AddColumnstoreIndexObjects(
                        index,
                        relation,
                        columns,
                        ownedColumns,
                        indexObjectId,
                        objects);
                    AddSubtypeDiagnostic(
                        indexObjectId,
                        "MIG-SQLSERVER-COLUMNSTORE-INDEX-UNSUPPORTED-001",
                        "The SQL Server columnstore index is not target-compatible.",
                        "Columnstore membership, ordering, data clustering, and compression behavior have no advertised CSharpDB lowering contract.",
                        diagnostics);
                    break;

                case 7:
                    metadataComplete = hashIndexes.TryGetValue(
                        key,
                        out SqlServerHashIndexMetadata? hashIndex);
                    if (hashIndex is not null)
                    {
                        AddHashIndexObject(
                            index,
                            hashIndex,
                            relation,
                            columns,
                            ownedColumns,
                            indexObjectId,
                            objects);
                    }
                    AddSubtypeDiagnostic(
                        indexObjectId,
                        "MIG-SQLSERVER-HASH-INDEX-UNSUPPORTED-001",
                        "The SQL Server hash index is not target-compatible.",
                        "Memory-optimized hash lookup and bucket-count semantics have no advertised CSharpDB lowering contract.",
                        diagnostics);
                    break;

                default:
                    jsonIndexes.TryGetValue(
                        key,
                        out SqlServerJsonIndexMetadata? jsonIndex);
                    jsonPaths.TryGetValue(
                        key,
                        out SqlServerJsonIndexPathMetadata[]? ownedJsonPaths);
                    metadataComplete =
                        jsonIndex is not null &&
                        ownedJsonPaths is { Length: > 0 };
                    if (jsonIndex is not null)
                    {
                        AddJsonIndexObjects(
                            index,
                            jsonIndex,
                            ownedJsonPaths ?? [],
                            relation,
                            columns,
                            ownedColumns,
                            indexObjectId,
                            objects);
                    }
                    AddSubtypeDiagnostic(
                        indexObjectId,
                        "MIG-SQLSERVER-JSON-INDEX-UNSUPPORTED-001",
                        "The SQL Server JSON index is not target-compatible.",
                        "SQL Server 2025 JSON path and array-search semantics have no advertised CSharpDB lowering contract.",
                        diagnostics);
                    break;
            }

            if (!metadataComplete)
            {
                diagnostics.Add(Diagnostic(
                    indexObjectId,
                    IndexSubtypeMetadataIncompleteRule,
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unknown,
                    "The SQL Server index-subtype configuration metadata is incomplete.",
                    "The base index remained visible, but one or more subtype rows, paths, tessellation facts, or versioned columnstore ordinals were hidden from this restricted inspection.",
                    "Restore complete metadata visibility and inspect again.",
                    canOverride: false));
            }
        }
    }

    private static void AddXmlIndexObjects(
        SqlServerIndexMetadata index,
        SqlServerXmlIndexMetadata xmlIndex,
        IReadOnlyList<SqlServerSelectiveXmlIndexPathMetadata> paths,
        PhysicalRelation relation,
        IReadOnlyDictionary<(int ObjectId, int ColumnId), PhysicalColumn> columns,
        IReadOnlyList<SqlServerIndexColumnMetadata> indexColumns,
        IReadOnlyDictionary<(int ObjectId, int IndexId), string>
            nativeIndexObjectIds,
        string indexObjectId,
        ICollection<MigrationCatalogObject> objects)
    {
        string configId = ObjectId(
            "xml-index-config",
            relation.SchemaName,
            relation.Name,
            index.Name);
        var dependencies = ResolveSubtypeColumnDependencies(
            index,
            indexColumns,
            columns).ToList();
        if (xmlIndex.UsingXmlIndexId is int usingIndexId &&
            nativeIndexObjectIds.TryGetValue(
                (index.ObjectId, usingIndexId),
                out string? usingIndexObjectId))
        {
            dependencies.Add(usingIndexObjectId);
        }
        string[] distinctDependencies = dependencies
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        objects.Add(new MigrationCatalogObject
        {
            ObjectId = configId,
            Kind = MigrationObjectKind.Other,
            ParentObjectId = indexObjectId,
            SourceNamespace = relation.SchemaName,
            SourceName = "$xml-config",
            Facets =
            [
                Facet("sqlServerObjectClass", "xml-index-config"),
                Facet("sqlServerObjectId", Invariant(xmlIndex.ObjectId)),
                Facet("sqlServerIndexId", Invariant(xmlIndex.IndexId)),
                Facet(
                    "sqlServerUsingXmlIndexId",
                    OptionalInvariant(xmlIndex.UsingXmlIndexId)),
                Facet("sqlServerSecondaryType", xmlIndex.SecondaryType),
                Facet(
                    "sqlServerSecondaryTypeDescription",
                    xmlIndex.SecondaryTypeDescription),
                Facet("sqlServerXmlIndexType", Invariant(xmlIndex.XmlIndexType)),
                Facet(
                    "sqlServerXmlIndexTypeDescription",
                    xmlIndex.XmlIndexTypeDescription),
                Facet("sqlServerPathId", OptionalInvariant(xmlIndex.PathId)),
            ],
            DependsOn = distinctDependencies,
        });

        foreach (SqlServerSelectiveXmlIndexPathMetadata path in paths)
        {
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = ObjectId(
                    "selective-xml-index-path",
                    relation.SchemaName,
                    relation.Name,
                    index.Name,
                    Invariant(path.PathId)),
                Kind = MigrationObjectKind.Other,
                ParentObjectId = configId,
                SourceNamespace = relation.SchemaName,
                SourceName = path.Name,
                Facets =
                [
                    Facet(
                        "sqlServerObjectClass",
                        "selective-xml-index-path"),
                    Facet("sqlServerPathId", Invariant(path.PathId)),
                    Facet("sqlServerPathSourceBytes", Invariant(path.PathBytes)),
                    Facet(
                        "sqlServerPathDigest",
                        "sha256:" + SqlServerStableDigest.Text(
                            "csharpdb-sqlserver-selective-xml-index-path/v1",
                            path.Path)),
                    Facet("sqlServerPathLength", Invariant(path.Path.Length)),
                    Facet("sqlServerPathType", Invariant(path.PathType)),
                    Facet(
                        "sqlServerPathTypeDescription",
                        path.PathTypeDescription),
                    Facet(
                        "sqlServerXmlComponentId",
                        OptionalInvariant(path.XmlComponentId)),
                    Facet(
                        "sqlServerXQueryTypeDescription",
                        path.XQueryTypeDescription),
                    Facet(
                        "sqlServerXQueryTypeInferred",
                        NullableBoolean(path.IsXQueryTypeInferred)),
                    Facet(
                        "sqlServerXQueryMaximumLength",
                        OptionalInvariant(path.XQueryMaximumLength)),
                    Facet(
                        "sqlServerXQueryMaximumLengthInferred",
                        NullableBoolean(path.IsXQueryMaximumLengthInferred)),
                    Facet("sqlServerNodeHint", NullableBoolean(path.IsNode)),
                    Facet(
                        "sqlServerSystemTypeId",
                        OptionalInvariant(path.SystemTypeId)),
                    Facet(
                        "sqlServerUserTypeId",
                        OptionalInvariant(path.UserTypeId)),
                    Facet(
                        "sqlServerMaxLength",
                        OptionalInvariant(path.MaxLength)),
                    Facet(
                        "sqlServerPrecision",
                        OptionalInvariant(path.Precision)),
                    Facet("sqlServerScale", OptionalInvariant(path.Scale)),
                    Facet("sqlServerCollation", path.Collation),
                    Facet(
                        "sqlServerSingletonHint",
                        NullableBoolean(path.IsSingleton)),
                ],
            });
        }
    }

    private static void AddSpatialIndexObjects(
        SqlServerIndexMetadata index,
        SqlServerSpatialIndexMetadata spatialIndex,
        SqlServerSpatialIndexTessellationMetadata? tessellation,
        PhysicalRelation relation,
        IReadOnlyDictionary<(int ObjectId, int ColumnId), PhysicalColumn> columns,
        IReadOnlyList<SqlServerIndexColumnMetadata> indexColumns,
        string indexObjectId,
        ICollection<MigrationCatalogObject> objects)
    {
        string configId = ObjectId(
            "spatial-index-config",
            relation.SchemaName,
            relation.Name,
            index.Name);
        objects.Add(new MigrationCatalogObject
        {
            ObjectId = configId,
            Kind = MigrationObjectKind.Other,
            ParentObjectId = indexObjectId,
            SourceNamespace = relation.SchemaName,
            SourceName = "$spatial-config",
            Facets =
            [
                Facet("sqlServerObjectClass", "spatial-index-config"),
                Facet(
                    "sqlServerSpatialIndexType",
                    Invariant(spatialIndex.SpatialIndexType)),
                Facet(
                    "sqlServerSpatialIndexTypeDescription",
                    spatialIndex.SpatialIndexTypeDescription),
                Facet(
                    "sqlServerTessellationScheme",
                    spatialIndex.TessellationScheme),
            ],
            DependsOn = ResolveSubtypeColumnDependencies(
                index,
                indexColumns,
                columns),
        });

        if (tessellation is null)
            return;
        objects.Add(new MigrationCatalogObject
        {
            ObjectId = ObjectId(
                "spatial-index-tessellation",
                relation.SchemaName,
                relation.Name,
                index.Name),
            Kind = MigrationObjectKind.Other,
            ParentObjectId = configId,
            SourceNamespace = relation.SchemaName,
            SourceName = "$tessellation",
            Facets =
            [
                Facet(
                    "sqlServerObjectClass",
                    "spatial-index-tessellation"),
                Facet(
                    "sqlServerTessellationScheme",
                    tessellation.TessellationScheme),
                Facet(
                    "sqlServerBoundingBoxXMin",
                    OptionalInvariant(tessellation.BoundingBoxXMin)),
                Facet(
                    "sqlServerBoundingBoxYMin",
                    OptionalInvariant(tessellation.BoundingBoxYMin)),
                Facet(
                    "sqlServerBoundingBoxXMax",
                    OptionalInvariant(tessellation.BoundingBoxXMax)),
                Facet(
                    "sqlServerBoundingBoxYMax",
                    OptionalInvariant(tessellation.BoundingBoxYMax)),
                Facet(
                    "sqlServerLevel1Grid",
                    OptionalInvariant(tessellation.Level1Grid)),
                Facet(
                    "sqlServerLevel1GridDescription",
                    tessellation.Level1GridDescription),
                Facet(
                    "sqlServerLevel2Grid",
                    OptionalInvariant(tessellation.Level2Grid)),
                Facet(
                    "sqlServerLevel2GridDescription",
                    tessellation.Level2GridDescription),
                Facet(
                    "sqlServerLevel3Grid",
                    OptionalInvariant(tessellation.Level3Grid)),
                Facet(
                    "sqlServerLevel3GridDescription",
                    tessellation.Level3GridDescription),
                Facet(
                    "sqlServerLevel4Grid",
                    OptionalInvariant(tessellation.Level4Grid)),
                Facet(
                    "sqlServerLevel4GridDescription",
                    tessellation.Level4GridDescription),
                Facet(
                    "sqlServerCellsPerObject",
                    OptionalInvariant(tessellation.CellsPerObject)),
            ],
        });
    }

    private static void AddHashIndexObject(
        SqlServerIndexMetadata index,
        SqlServerHashIndexMetadata hashIndex,
        PhysicalRelation relation,
        IReadOnlyDictionary<(int ObjectId, int ColumnId), PhysicalColumn> columns,
        IReadOnlyList<SqlServerIndexColumnMetadata> indexColumns,
        string indexObjectId,
        ICollection<MigrationCatalogObject> objects)
    {
        objects.Add(new MigrationCatalogObject
        {
            ObjectId = ObjectId(
                "hash-index-config",
                relation.SchemaName,
                relation.Name,
                index.Name),
            Kind = MigrationObjectKind.Other,
            ParentObjectId = indexObjectId,
            SourceNamespace = relation.SchemaName,
            SourceName = "$hash-config",
            Facets =
            [
                Facet("sqlServerObjectClass", "hash-index-config"),
                Facet("sqlServerBucketCount", Invariant(hashIndex.BucketCount)),
            ],
            DependsOn = ResolveSubtypeColumnDependencies(
                index,
                indexColumns,
                columns),
        });
    }

    private static void AddColumnstoreIndexObjects(
        SqlServerIndexMetadata index,
        PhysicalRelation relation,
        IReadOnlyDictionary<(int ObjectId, int ColumnId), PhysicalColumn> columns,
        IReadOnlyList<SqlServerIndexColumnMetadata> indexColumns,
        string indexObjectId,
        ICollection<MigrationCatalogObject> objects)
    {
        string configId = ObjectId(
            "columnstore-index-config",
            relation.SchemaName,
            relation.Name,
            index.Name);
        objects.Add(new MigrationCatalogObject
        {
            ObjectId = configId,
            Kind = MigrationObjectKind.Other,
            ParentObjectId = indexObjectId,
            SourceNamespace = relation.SchemaName,
            SourceName = "$columnstore-config",
            Facets =
            [
                Facet("sqlServerObjectClass", "columnstore-index-config"),
                Facet(
                    "sqlServerColumnstoreKind",
                    index.Type == 5 ? "clustered" : "nonclustered"),
                Facet(
                    "sqlServerOrdered",
                    Boolean(indexColumns.Any(static item =>
                        item.ColumnStoreOrderOrdinal > 0))),
                Facet(
                    "sqlServerDataClustered",
                    Boolean(indexColumns.Any(static item =>
                        item.DataClusteringOrdinal > 0))),
            ],
            DependsOn = ResolveSubtypeColumnDependencies(
                index,
                indexColumns,
                columns),
        });

        foreach (SqlServerIndexColumnMetadata column in indexColumns
                     .Where(item => columns.ContainsKey(
                         (item.ObjectId, item.ColumnId)))
                     .OrderBy(static item => item.IndexColumnId))
        {
            PhysicalColumn sourceColumn =
                columns[(column.ObjectId, column.ColumnId)];
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = ObjectId(
                    "columnstore-index-column",
                    relation.SchemaName,
                    relation.Name,
                    index.Name,
                    Invariant(column.IndexColumnId)),
                Kind = MigrationObjectKind.Other,
                ParentObjectId = configId,
                SourceNamespace = relation.SchemaName,
                SourceName = sourceColumn.Name,
                Facets =
                [
                    Facet(
                        "sqlServerObjectClass",
                        "columnstore-index-column"),
                    Facet(
                        "sqlServerIndexColumnId",
                        Invariant(column.IndexColumnId)),
                    Facet("sqlServerColumnId", Invariant(column.ColumnId)),
                    Facet(
                        "sqlServerPartitionOrdinal",
                        Invariant(column.PartitionOrdinal)),
                    Facet(
                        "sqlServerColumnStoreOrderOrdinal",
                        OptionalInvariant(column.ColumnStoreOrderOrdinal)),
                    Facet(
                        "sqlServerDataClusteringOrdinal",
                        OptionalInvariant(column.DataClusteringOrdinal)),
                ],
                DependsOn = [sourceColumn.Id],
            });
        }
    }

    private static void AddJsonIndexObjects(
        SqlServerIndexMetadata index,
        SqlServerJsonIndexMetadata jsonIndex,
        IReadOnlyList<SqlServerJsonIndexPathMetadata> paths,
        PhysicalRelation relation,
        IReadOnlyDictionary<(int ObjectId, int ColumnId), PhysicalColumn> columns,
        IReadOnlyList<SqlServerIndexColumnMetadata> indexColumns,
        string indexObjectId,
        ICollection<MigrationCatalogObject> objects)
    {
        string configId = ObjectId(
            "json-index-config",
            relation.SchemaName,
            relation.Name,
            index.Name);
        objects.Add(new MigrationCatalogObject
        {
            ObjectId = configId,
            Kind = MigrationObjectKind.Other,
            ParentObjectId = indexObjectId,
            SourceNamespace = relation.SchemaName,
            SourceName = "$json-config",
            Facets =
            [
                Facet("sqlServerObjectClass", "json-index-config"),
                Facet(
                    "sqlServerOptimizeForArraySearch",
                    Boolean(jsonIndex.OptimizeForArraySearch)),
            ],
            DependsOn = ResolveSubtypeColumnDependencies(
                index,
                indexColumns,
                columns),
        });

        foreach (SqlServerJsonIndexPathMetadata path in paths)
        {
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = ObjectId(
                    "json-index-path",
                    relation.SchemaName,
                    relation.Name,
                    index.Name,
                    Invariant(path.PathOrdinal)),
                Kind = MigrationObjectKind.Other,
                ParentObjectId = configId,
                SourceNamespace = relation.SchemaName,
                SourceName = "$path-" + Invariant(path.PathOrdinal),
                Facets =
                [
                    Facet("sqlServerObjectClass", "json-index-path"),
                    Facet(
                        "sqlServerPathOrdinal",
                        Invariant(path.PathOrdinal)),
                    Facet("sqlServerPathSourceBytes", Invariant(path.PathBytes)),
                    Facet(
                        "sqlServerPathDigest",
                        "sha256:" + SqlServerStableDigest.Text(
                            "csharpdb-sqlserver-json-index-path/v1",
                            path.Path)),
                    Facet("sqlServerPathLength", Invariant(path.Path.Length)),
                ],
            });
        }
    }

    private static string[] ResolveSubtypeColumnDependencies(
        SqlServerIndexMetadata index,
        IReadOnlyList<SqlServerIndexColumnMetadata> indexColumns,
        IReadOnlyDictionary<(int ObjectId, int ColumnId), PhysicalColumn> columns) =>
        SubtypeMemberColumns(index, indexColumns)
            .Where(item => columns.ContainsKey((item.ObjectId, item.ColumnId)))
            .Select(item => columns[(item.ObjectId, item.ColumnId)].Id)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

    private static IEnumerable<SqlServerIndexColumnMetadata> SubtypeMemberColumns(
        SqlServerIndexMetadata index,
        IReadOnlyList<SqlServerIndexColumnMetadata> columns) =>
        index.Type switch
        {
            3 or 4 or 9 => columns
                .Where(static item =>
                    item.ColumnId > 0 &&
                    item.PartitionOrdinal == 0)
                .OrderBy(static item => item.IndexColumnId),
            5 or 6 => columns
                .Where(static item => item.ColumnId > 0)
                .OrderBy(static item => item.IndexColumnId),
            _ => columns
                .Where(static item =>
                    item.ColumnId > 0 &&
                    item.KeyOrdinal > 0 &&
                    !item.IsIncluded)
                .OrderBy(static item => item.KeyOrdinal)
                .ThenBy(static item => item.IndexColumnId),
        };

    private static void AddSubtypeDiagnostic(
        string objectId,
        string ruleId,
        string summary,
        string explanation,
        ICollection<MigrationDiagnostic> diagnostics)
    {
        diagnostics.Add(Diagnostic(
            objectId,
            ruleId,
            MigrationDiagnosticSeverity.Error,
            MigrationCompatibilityStatus.Unsupported,
            summary,
            explanation,
            "Choose and differentially validate an explicit target index design.",
            canOverride: false));
    }

    private static void ValidateIndexSubtypeCounts(
        SqlServerCatalogSnapshot snapshot,
        SqlServerInspectionLimits limits)
    {
        if (snapshot.XmlIndexes.Count > limits.MaxXmlIndexes)
            throw LimitExceeded("XML-index count");
        if (snapshot.SelectiveXmlIndexPaths.Count >
            limits.MaxSelectiveXmlIndexPaths)
        {
            throw LimitExceeded("selective-XML-index path count");
        }
        if (snapshot.SpatialIndexes.Count > limits.MaxSpatialIndexes)
            throw LimitExceeded("spatial-index count");
        if (snapshot.SpatialIndexTessellations.Count >
            limits.MaxSpatialIndexTessellations)
        {
            throw LimitExceeded("spatial-index tessellation count");
        }
        if (snapshot.HashIndexes.Count > limits.MaxHashIndexes)
            throw LimitExceeded("hash-index count");
        if (snapshot.JsonIndexes.Count > limits.MaxJsonIndexes)
            throw LimitExceeded("JSON-index count");
        if (snapshot.JsonIndexPaths.Count > limits.MaxJsonIndexPaths)
            throw LimitExceeded("JSON-index path count");
    }

    private static void ValidateIndexSubtypeSnapshot(
        SqlServerCatalogSnapshot snapshot,
        IReadOnlySet<int> tableIds,
        IReadOnlySet<(int ObjectId, int ColumnId)> tableColumnIds,
        MetadataBudget budget,
        SqlServerInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        Dictionary<(int ObjectId, int IndexId), SqlServerIndexMetadata> indexes =
            snapshot.Indexes.ToDictionary(
                static item => (item.ObjectId, item.IndexId));
        Dictionary<(int ObjectId, int IndexId), SqlServerIndexColumnMetadata[]>
            indexColumns = snapshot.IndexColumns
                .GroupBy(static item => (item.ObjectId, item.IndexId))
                .ToDictionary(
                    static group => group.Key,
                    static group => group.ToArray());
        bool visibilityComplete =
            GetMetadataVisibility(snapshot) == MetadataVisibility.Complete;

        Dictionary<(int ObjectId, int IndexId), SqlServerXmlIndexMetadata>
            xmlIndexes = ValidateXmlIndexes(
                snapshot,
                indexes,
                tableIds,
                budget,
                visibilityComplete,
                cancellationToken);
        Dictionary<(int ObjectId, int IndexId),
            SqlServerSelectiveXmlIndexPathMetadata[]> xmlPaths =
            ValidateSelectiveXmlIndexPaths(
                snapshot,
                indexes,
                xmlIndexes,
                budget,
                limits,
                visibilityComplete,
                cancellationToken);
        Dictionary<(int ObjectId, int IndexId), SqlServerSpatialIndexMetadata>
            spatialIndexes = ValidateSpatialIndexes(
                snapshot,
                indexes,
                tableIds,
                budget,
                cancellationToken);
        Dictionary<(int ObjectId, int IndexId),
            SqlServerSpatialIndexTessellationMetadata> spatialTessellations =
            ValidateSpatialTessellations(
                snapshot,
                spatialIndexes,
                budget,
                cancellationToken);
        Dictionary<(int ObjectId, int IndexId), SqlServerHashIndexMetadata>
            hashIndexes = ValidateHashIndexes(
                snapshot,
                indexes,
                tableIds,
                cancellationToken);
        Dictionary<(int ObjectId, int IndexId), SqlServerJsonIndexMetadata>
            jsonIndexes = ValidateJsonIndexes(
                snapshot,
                indexes,
                tableIds,
                cancellationToken);
        Dictionary<(int ObjectId, int IndexId), SqlServerJsonIndexPathMetadata[]>
            jsonPaths = ValidateJsonIndexPaths(
                snapshot,
                jsonIndexes,
                budget,
                limits,
                cancellationToken);

        ValidateColumnstoreIndexColumns(
            snapshot,
            indexes,
            tableColumnIds,
            visibilityComplete,
            cancellationToken);

        foreach (SqlServerIndexMetadata index in snapshot.Indexes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            (int ObjectId, int IndexId) key = (index.ObjectId, index.IndexId);
            if (index.Type is 3 or 4 or 5 or 6 or 7 or 9 &&
                !tableIds.Contains(index.ObjectId))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned an index subtype outside a visible user table.");
            }

            bool subtypeComplete = index.Type switch
            {
                3 => xmlIndexes.TryGetValue(
                         key,
                         out SqlServerXmlIndexMetadata? xmlIndex) &&
                     HasCompleteXmlIndexMetadata(
                         key,
                         xmlIndex,
                         xmlIndexes,
                         xmlPaths),
                4 => spatialIndexes.ContainsKey(key) &&
                     spatialTessellations.ContainsKey(key),
                5 or 6 => indexColumns.TryGetValue(
                              key,
                              out SqlServerIndexColumnMetadata[]?
                                  ownedIndexColumns) &&
                          HasCompleteColumnstoreMetadata(
                              snapshot.Instance.ProductMajorVersion,
                              ownedIndexColumns),
                7 => hashIndexes.ContainsKey(key),
                9 => jsonIndexes.ContainsKey(key) &&
                     jsonPaths.TryGetValue(
                         key,
                         out SqlServerJsonIndexPathMetadata[]? paths) &&
                     paths.Length > 0,
                _ => true,
            };
            if (visibilityComplete && !subtypeComplete)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned incomplete index-subtype metadata.");
            }
        }
    }

    private static bool HasCompleteXmlIndexMetadata(
        (int ObjectId, int IndexId) key,
        SqlServerXmlIndexMetadata? xmlIndex,
        IReadOnlyDictionary<(int ObjectId, int IndexId),
            SqlServerXmlIndexMetadata> xmlIndexes,
        IReadOnlyDictionary<(int ObjectId, int IndexId),
            SqlServerSelectiveXmlIndexPathMetadata[]> paths)
    {
        if (xmlIndex is null)
            return false;
        if (xmlIndex.UsingXmlIndexId is int usingIndexId &&
            !xmlIndexes.ContainsKey((key.ObjectId, usingIndexId)))
        {
            return false;
        }
        if (xmlIndex.XmlIndexType == 2)
        {
            return paths.TryGetValue(
                       key,
                       out SqlServerSelectiveXmlIndexPathMetadata[]?
                           selectivePaths) &&
                   selectivePaths.Length > 0;
        }
        if (xmlIndex.XmlIndexType == 3)
        {
            return xmlIndex.UsingXmlIndexId is int parentIndexId &&
                   xmlIndex.PathId is int pathId &&
                   paths.TryGetValue(
                       (key.ObjectId, parentIndexId),
                       out SqlServerSelectiveXmlIndexPathMetadata[]?
                           selectivePaths) &&
                   selectivePaths.Any(item => item.PathId == pathId);
        }
        return true;
    }

    private static Dictionary<(int ObjectId, int IndexId),
        SqlServerXmlIndexMetadata> ValidateXmlIndexes(
        SqlServerCatalogSnapshot snapshot,
        IReadOnlyDictionary<(int ObjectId, int IndexId), SqlServerIndexMetadata>
            indexes,
        IReadOnlySet<int> tableIds,
        MetadataBudget budget,
        bool visibilityComplete,
        CancellationToken cancellationToken)
    {
        var result = new Dictionary<
            (int ObjectId, int IndexId),
            SqlServerXmlIndexMetadata>();
        foreach (SqlServerXmlIndexMetadata xmlIndex in snapshot.XmlIndexes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            (int ObjectId, int IndexId) key =
                (xmlIndex.ObjectId, xmlIndex.IndexId);
            if (!tableIds.Contains(xmlIndex.ObjectId) ||
                !indexes.TryGetValue(key, out SqlServerIndexMetadata? index) ||
                index.Type != 3 ||
                !result.TryAdd(key, xmlIndex) ||
                xmlIndex.XmlIndexType > 3 ||
                xmlIndex.UsingXmlIndexId is <= 0 ||
                xmlIndex.PathId is <= 0 ||
                (xmlIndex.SecondaryType is null) !=
                (xmlIndex.SecondaryTypeDescription is null))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned XML-index metadata.");
            }
            budget.Add(xmlIndex.SecondaryType);
            budget.Add(xmlIndex.SecondaryTypeDescription);
            budget.Add(xmlIndex.XmlIndexTypeDescription);
        }

        foreach (SqlServerXmlIndexMetadata xmlIndex in result.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();
            bool shapeValid = xmlIndex.XmlIndexType switch
            {
                0 => xmlIndex.UsingXmlIndexId is null &&
                     xmlIndex.SecondaryType is null &&
                     xmlIndex.PathId is null,
                1 => xmlIndex.UsingXmlIndexId is int &&
                     xmlIndex.SecondaryType is "P" or "V" or "R" &&
                     xmlIndex.PathId is null,
                2 => xmlIndex.UsingXmlIndexId is null &&
                     xmlIndex.SecondaryType is null &&
                     xmlIndex.PathId is null,
                3 => xmlIndex.UsingXmlIndexId is int &&
                     xmlIndex.SecondaryType is null &&
                     xmlIndex.PathId is int,
                _ => false,
            };
            if (!shapeValid)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned inconsistent XML-index subtype metadata.");
            }
            if (xmlIndex.UsingXmlIndexId is not int usingIndexId)
                continue;
            if (!result.TryGetValue(
                    (xmlIndex.ObjectId, usingIndexId),
                    out SqlServerXmlIndexMetadata? usingIndex))
            {
                if (visibilityComplete)
                {
                    throw new SqlServerMigrationException(
                        "SQL Server returned an invalid XML-index parent reference.");
                }
                continue;
            }
            if (usingIndex.IndexId == xmlIndex.IndexId ||
                xmlIndex.XmlIndexType == 1 && usingIndex.XmlIndexType != 0 ||
                xmlIndex.XmlIndexType == 3 && usingIndex.XmlIndexType != 2)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned an invalid XML-index parent reference.");
            }
        }
        return result;
    }

    private static Dictionary<(int ObjectId, int IndexId),
        SqlServerSelectiveXmlIndexPathMetadata[]> ValidateSelectiveXmlIndexPaths(
        SqlServerCatalogSnapshot snapshot,
        IReadOnlyDictionary<(int ObjectId, int IndexId), SqlServerIndexMetadata>
            indexes,
        IReadOnlyDictionary<(int ObjectId, int IndexId), SqlServerXmlIndexMetadata>
            xmlIndexes,
        MetadataBudget budget,
        SqlServerInspectionLimits limits,
        bool visibilityComplete,
        CancellationToken cancellationToken)
    {
        var pathsByIndex = new Dictionary<
            (int ObjectId, int IndexId),
            SortedDictionary<int, SqlServerSelectiveXmlIndexPathMetadata>>();
        var pathNames = new HashSet<(int ObjectId, int IndexId, string Name)>();
        foreach (SqlServerSelectiveXmlIndexPathMetadata path in
                 snapshot.SelectiveXmlIndexPaths)
        {
            cancellationToken.ThrowIfCancellationRequested();
            (int ObjectId, int IndexId) key = (path.ObjectId, path.IndexId);
            xmlIndexes.TryGetValue(
                key,
                out SqlServerXmlIndexMetadata? xmlIndex);
            if (!indexes.TryGetValue(
                    key,
                    out SqlServerIndexMetadata? baseIndex) ||
                baseIndex.Type != 3 ||
                xmlIndex is not null && xmlIndex.XmlIndexType != 2 ||
                visibilityComplete && xmlIndex is null ||
                path.PathId <= 0 ||
                path.PathType > 1 ||
                path.PathBytes <= 0 ||
                path.PathBytes > limits.MaxIndexPathBytes ||
                string.IsNullOrEmpty(path.Path) ||
                path.Path.Length > limits.MaxIndexPathBytes / 2 ||
                string.IsNullOrWhiteSpace(path.Name) ||
                !pathNames.Add((path.ObjectId, path.IndexId, path.Name)) ||
                path.XmlComponentId is < 0 ||
                path.XQueryMaximumLength is < -1 ||
                path.SystemTypeId is 0 ||
                path.UserTypeId is <= 0 ||
                path.MaxLength is < -1 ||
                path.Precision is > 53 ||
                path.Scale is > 38)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned invalid or unowned selective-XML-index path metadata.");
            }
            if (path.PathBytes != checked(path.Path.Length * 2))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned inconsistent selective-XML-index path length metadata.");
            }
            if (!pathsByIndex.TryGetValue(
                    key,
                    out SortedDictionary<
                        int,
                        SqlServerSelectiveXmlIndexPathMetadata>? paths))
            {
                paths = [];
                pathsByIndex.Add(key, paths);
            }
            if (!paths.TryAdd(path.PathId, path))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate selective-XML-index path metadata.");
            }
            budget.Add(path.Path);
            budget.Add(path.Name, isName: true);
            budget.Add(path.PathTypeDescription);
            budget.Add(path.XQueryTypeDescription);
            budget.Add(path.Collation);
        }

        return pathsByIndex.ToDictionary(
            static item => item.Key,
            static item => item.Value.Values.ToArray());
    }

    private static Dictionary<(int ObjectId, int IndexId),
        SqlServerSpatialIndexMetadata> ValidateSpatialIndexes(
        SqlServerCatalogSnapshot snapshot,
        IReadOnlyDictionary<(int ObjectId, int IndexId), SqlServerIndexMetadata>
            indexes,
        IReadOnlySet<int> tableIds,
        MetadataBudget budget,
        CancellationToken cancellationToken)
    {
        var result = new Dictionary<
            (int ObjectId, int IndexId),
            SqlServerSpatialIndexMetadata>();
        foreach (SqlServerSpatialIndexMetadata spatialIndex in
                 snapshot.SpatialIndexes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            (int ObjectId, int IndexId) key =
                (spatialIndex.ObjectId, spatialIndex.IndexId);
            bool validScheme = spatialIndex.TessellationScheme is
                "GEOMETRY_GRID" or "GEOMETRY_AUTO_GRID" or
                "GEOGRAPHY_GRID" or "GEOGRAPHY_AUTO_GRID";
            bool schemeMatchesType =
                spatialIndex.SpatialIndexType == 1 &&
                spatialIndex.TessellationScheme.StartsWith(
                    "GEOMETRY_",
                    StringComparison.Ordinal) ||
                spatialIndex.SpatialIndexType == 2 &&
                spatialIndex.TessellationScheme.StartsWith(
                    "GEOGRAPHY_",
                    StringComparison.Ordinal);
            if (!tableIds.Contains(spatialIndex.ObjectId) ||
                !indexes.TryGetValue(key, out SqlServerIndexMetadata? index) ||
                index.Type != 4 ||
                !result.TryAdd(key, spatialIndex) ||
                !validScheme ||
                !schemeMatchesType)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned spatial-index metadata.");
            }
            budget.Add(spatialIndex.SpatialIndexTypeDescription);
            budget.Add(spatialIndex.TessellationScheme);
        }
        return result;
    }

    private static Dictionary<(int ObjectId, int IndexId),
        SqlServerSpatialIndexTessellationMetadata>
        ValidateSpatialTessellations(
            SqlServerCatalogSnapshot snapshot,
            IReadOnlyDictionary<(int ObjectId, int IndexId),
                SqlServerSpatialIndexMetadata> spatialIndexes,
            MetadataBudget budget,
            CancellationToken cancellationToken)
    {
        var result = new Dictionary<
            (int ObjectId, int IndexId),
            SqlServerSpatialIndexTessellationMetadata>();
        foreach (SqlServerSpatialIndexTessellationMetadata tessellation in
                 snapshot.SpatialIndexTessellations)
        {
            cancellationToken.ThrowIfCancellationRequested();
            (int ObjectId, int IndexId) key =
                (tessellation.ObjectId, tessellation.IndexId);
            if (!spatialIndexes.TryGetValue(
                    key,
                    out SqlServerSpatialIndexMetadata? spatialIndex) ||
                !result.TryAdd(key, tessellation) ||
                tessellation.TessellationScheme is not
                    ("GEOMETRY_GRID" or "GEOGRAPHY_GRID") ||
                !string.Equals(
                    NormalizeSpatialTessellationScheme(
                        spatialIndex.TessellationScheme),
                    NormalizeSpatialTessellationScheme(
                        tessellation.TessellationScheme),
                    StringComparison.Ordinal) ||
                !ValidSpatialTessellation(spatialIndex, tessellation))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned spatial tessellation metadata.");
            }
            budget.Add(tessellation.TessellationScheme);
            budget.Add(tessellation.Level1GridDescription);
            budget.Add(tessellation.Level2GridDescription);
            budget.Add(tessellation.Level3GridDescription);
            budget.Add(tessellation.Level4GridDescription);
        }
        return result;
    }

    private static bool ValidSpatialTessellation(
        SqlServerSpatialIndexMetadata spatialIndex,
        SqlServerSpatialIndexTessellationMetadata tessellation)
    {
        double?[] bounds =
        [
            tessellation.BoundingBoxXMin,
            tessellation.BoundingBoxYMin,
            tessellation.BoundingBoxXMax,
            tessellation.BoundingBoxYMax,
        ];
        bool anyBounds = bounds.Any(static item => item is not null);
        bool allBounds = bounds.All(static item =>
            item is double value && double.IsFinite(value));
        if (anyBounds != allBounds ||
            allBounds &&
            (tessellation.BoundingBoxXMin >= tessellation.BoundingBoxXMax ||
             tessellation.BoundingBoxYMin >= tessellation.BoundingBoxYMax))
        {
            return false;
        }

        bool autoGrid = spatialIndex.TessellationScheme.EndsWith(
            "_AUTO_GRID",
            StringComparison.Ordinal);
        bool geography = spatialIndex.TessellationScheme.StartsWith(
            "GEOGRAPHY_",
            StringComparison.Ordinal);
        (short? Code, string? Description)[] grids =
        [
            (tessellation.Level1Grid, tessellation.Level1GridDescription),
            (tessellation.Level2Grid, tessellation.Level2GridDescription),
            (tessellation.Level3Grid, tessellation.Level3GridDescription),
            (tessellation.Level4Grid, tessellation.Level4GridDescription),
        ];
        if ((geography ? anyBounds : !allBounds) ||
            tessellation.CellsPerObject is not (>= 1 and <= 8_192))
        {
            return false;
        }
        if (autoGrid)
        {
            return grids.All(static item =>
                item.Code is null && item.Description is null);
        }
        return grids.All(static item => GridMatchesDescription(
            item.Code,
            item.Description));
    }

    private static string NormalizeSpatialTessellationScheme(string value) =>
        value switch
        {
            "GEOMETRY_AUTO_GRID" => "GEOMETRY_GRID",
            "GEOGRAPHY_AUTO_GRID" => "GEOGRAPHY_GRID",
            _ => value,
        };

    private static bool GridMatchesDescription(
        short? code,
        string? description) =>
        (code, description) switch
        {
            (16, "LOW") => true,
            (64, "MEDIUM") => true,
            (256, "HIGH") => true,
            _ => false,
        };

    private static Dictionary<(int ObjectId, int IndexId),
        SqlServerHashIndexMetadata> ValidateHashIndexes(
        SqlServerCatalogSnapshot snapshot,
        IReadOnlyDictionary<(int ObjectId, int IndexId), SqlServerIndexMetadata>
            indexes,
        IReadOnlySet<int> tableIds,
        CancellationToken cancellationToken)
    {
        HashSet<int> memoryOptimizedTables = snapshot.Tables
            .Where(static item => item.IsMemoryOptimized)
            .Select(static item => item.ObjectId)
            .ToHashSet();
        var result = new Dictionary<
            (int ObjectId, int IndexId),
            SqlServerHashIndexMetadata>();
        foreach (SqlServerHashIndexMetadata hashIndex in snapshot.HashIndexes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            (int ObjectId, int IndexId) key =
                (hashIndex.ObjectId, hashIndex.IndexId);
            if (!tableIds.Contains(hashIndex.ObjectId) ||
                !memoryOptimizedTables.Contains(hashIndex.ObjectId) ||
                !indexes.TryGetValue(key, out SqlServerIndexMetadata? index) ||
                index.Type != 7 ||
                hashIndex.BucketCount is < 1 or > 1_073_741_824 ||
                !result.TryAdd(key, hashIndex))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned hash-index metadata.");
            }
        }
        return result;
    }

    private static Dictionary<(int ObjectId, int IndexId),
        SqlServerJsonIndexMetadata> ValidateJsonIndexes(
        SqlServerCatalogSnapshot snapshot,
        IReadOnlyDictionary<(int ObjectId, int IndexId), SqlServerIndexMetadata>
            indexes,
        IReadOnlySet<int> tableIds,
        CancellationToken cancellationToken)
    {
        if (snapshot.Instance.ProductMajorVersion < 17 &&
            (snapshot.JsonIndexes.Count > 0 ||
             snapshot.JsonIndexPaths.Count > 0 ||
             snapshot.Indexes.Any(static item => item.Type == 9)))
        {
            throw new SqlServerMigrationException(
                "SQL Server returned JSON-index metadata before SQL Server 2025.");
        }

        var result = new Dictionary<
            (int ObjectId, int IndexId),
            SqlServerJsonIndexMetadata>();
        foreach (SqlServerJsonIndexMetadata jsonIndex in snapshot.JsonIndexes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            (int ObjectId, int IndexId) key =
                (jsonIndex.ObjectId, jsonIndex.IndexId);
            if (!tableIds.Contains(jsonIndex.ObjectId) ||
                !indexes.TryGetValue(key, out SqlServerIndexMetadata? index) ||
                index.Type != 9 ||
                !result.TryAdd(key, jsonIndex))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned JSON-index metadata.");
            }
        }
        return result;
    }

    private static Dictionary<(int ObjectId, int IndexId),
        SqlServerJsonIndexPathMetadata[]> ValidateJsonIndexPaths(
        SqlServerCatalogSnapshot snapshot,
        IReadOnlyDictionary<(int ObjectId, int IndexId), SqlServerJsonIndexMetadata>
            jsonIndexes,
        MetadataBudget budget,
        SqlServerInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        var pathsByIndex = new Dictionary<
            (int ObjectId, int IndexId),
            SortedDictionary<int, SqlServerJsonIndexPathMetadata>>();
        var pathValues = new HashSet<(int ObjectId, int IndexId, string Path)>();
        foreach (SqlServerJsonIndexPathMetadata path in snapshot.JsonIndexPaths)
        {
            cancellationToken.ThrowIfCancellationRequested();
            (int ObjectId, int IndexId) key = (path.ObjectId, path.IndexId);
            if (!jsonIndexes.ContainsKey(key) ||
                path.PathOrdinal <= 0 ||
                path.PathBytes <= 0 ||
                path.PathBytes > limits.MaxIndexPathBytes ||
                string.IsNullOrEmpty(path.Path) ||
                path.Path.Length > limits.MaxIndexPathBytes ||
                !pathValues.Add((path.ObjectId, path.IndexId, path.Path)))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned invalid or unowned JSON-index path metadata.");
            }
            if (!pathsByIndex.TryGetValue(
                    key,
                    out SortedDictionary<
                        int,
                        SqlServerJsonIndexPathMetadata>? paths))
            {
                paths = [];
                pathsByIndex.Add(key, paths);
            }
            if (!paths.TryAdd(path.PathOrdinal, path))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate JSON-index path metadata.");
            }
            budget.Add(path.Path);
        }

        foreach (SortedDictionary<int, SqlServerJsonIndexPathMetadata> paths in
                 pathsByIndex.Values)
        {
            if (!paths.Keys.SequenceEqual(Enumerable.Range(1, paths.Count)))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned noncontiguous JSON-index path ordinals.");
            }
        }
        return pathsByIndex.ToDictionary(
            static item => item.Key,
            static item => item.Value.Values.ToArray());
    }

    private static void ValidateColumnstoreIndexColumns(
        SqlServerCatalogSnapshot snapshot,
        IReadOnlyDictionary<(int ObjectId, int IndexId), SqlServerIndexMetadata>
            indexes,
        IReadOnlySet<(int ObjectId, int ColumnId)> tableColumnIds,
        bool visibilityComplete,
        CancellationToken cancellationToken)
    {
        int productMajorVersion = snapshot.Instance.ProductMajorVersion;
        var columnsByIndex = snapshot.IndexColumns
            .GroupBy(static item => (item.ObjectId, item.IndexId))
            .ToDictionary(
                static group => group.Key,
                static group => group.ToArray());
        foreach (SqlServerIndexColumnMetadata column in snapshot.IndexColumns)
        {
            cancellationToken.ThrowIfCancellationRequested();
            SqlServerIndexMetadata index = indexes[(
                column.ObjectId,
                column.IndexId)];
            if (!tableColumnIds.Contains((column.ObjectId, column.ColumnId)) &&
                index.Type is 5 or 6)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned a columnstore member outside its table.");
            }

            bool columnstore = index.Type is 5 or 6;
            bool orderAvailable = column.ColumnStoreOrderOrdinal is not null;
            bool clusteringAvailable = column.DataClusteringOrdinal is not null;
            if (!columnstore &&
                (column.ColumnStoreOrderOrdinal > 0 ||
                 column.DataClusteringOrdinal > 0))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned invalid versioned index-column metadata.");
            }
            if (columnstore &&
                (productMajorVersion < 16 &&
                 column.ColumnStoreOrderOrdinal > 0 ||
                 productMajorVersion == 16 &&
                 index.Type == 6 &&
                 column.ColumnStoreOrderOrdinal > 0 ||
                 productMajorVersion < 17 &&
                 column.DataClusteringOrdinal > 0))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned invalid versioned columnstore metadata.");
            }
            if (columnstore &&
                visibilityComplete &&
                (productMajorVersion >= 16 != orderAvailable ||
                 productMajorVersion >= 17 != clusteringAvailable))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned incomplete versioned index-column metadata.");
            }
        }

        foreach (SqlServerIndexMetadata index in snapshot.Indexes
                     .Where(static item => item.Type is 5 or 6))
        {
            cancellationToken.ThrowIfCancellationRequested();
            columnsByIndex.TryGetValue(
                (index.ObjectId, index.IndexId),
                out SqlServerIndexColumnMetadata[]? columns);
            columns ??= [];
            ValidateContiguousPositiveOrdinals(
                columns
                    .Where(static item => item.ColumnStoreOrderOrdinal > 0)
                    .Select(static item =>
                        (int)item.ColumnStoreOrderOrdinal!.Value),
                "columnstore order");
            ValidateContiguousPositiveOrdinals(
                columns
                    .Where(static item => item.DataClusteringOrdinal > 0)
                    .Select(static item =>
                        (int)item.DataClusteringOrdinal!.Value),
                "columnstore data-clustering");
        }
    }

    private static bool HasCompleteColumnstoreMetadata(
        int productMajorVersion,
        IReadOnlyList<SqlServerIndexColumnMetadata> columns) =>
        columns.Count > 0 &&
        columns.All(item =>
            (productMajorVersion >= 16) ==
            (item.ColumnStoreOrderOrdinal is not null) &&
            (productMajorVersion >= 17) ==
            (item.DataClusteringOrdinal is not null));

    private static void ValidateContiguousPositiveOrdinals(
        IEnumerable<int> ordinals,
        string description)
    {
        int[] ordered = ordinals.Order().ToArray();
        if (ordered.Distinct().Count() != ordered.Length ||
            !ordered.SequenceEqual(Enumerable.Range(1, ordered.Length)))
        {
            throw new SqlServerMigrationException(
                $"SQL Server returned invalid {description} ordinals.");
        }
    }

    private static IEnumerable<string?> IndexSubtypeSnapshotFields(
        SqlServerCatalogSnapshot snapshot)
    {
        foreach (SqlServerXmlIndexMetadata index in snapshot.XmlIndexes
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.IndexId))
        {
            yield return "xml-index";
            yield return Invariant(index.ObjectId);
            yield return Invariant(index.IndexId);
            yield return OptionalInvariant(index.UsingXmlIndexId);
            yield return index.SecondaryType;
            yield return index.SecondaryTypeDescription;
            yield return Invariant(index.XmlIndexType);
            yield return index.XmlIndexTypeDescription;
            yield return OptionalInvariant(index.PathId);
        }
        foreach (SqlServerSelectiveXmlIndexPathMetadata path in
                 snapshot.SelectiveXmlIndexPaths
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.IndexId)
                     .ThenBy(static item => item.PathId))
        {
            yield return "selective-xml-index-path";
            yield return Invariant(path.ObjectId);
            yield return Invariant(path.IndexId);
            yield return Invariant(path.PathId);
            yield return Invariant(path.PathBytes);
            yield return path.Path;
            yield return path.Name;
            yield return Invariant(path.PathType);
            yield return path.PathTypeDescription;
            yield return OptionalInvariant(path.XmlComponentId);
            yield return path.XQueryTypeDescription;
            yield return NullableBoolean(path.IsXQueryTypeInferred);
            yield return OptionalInvariant(path.XQueryMaximumLength);
            yield return NullableBoolean(path.IsXQueryMaximumLengthInferred);
            yield return NullableBoolean(path.IsNode);
            yield return OptionalInvariant(path.SystemTypeId);
            yield return OptionalInvariant(path.UserTypeId);
            yield return OptionalInvariant(path.MaxLength);
            yield return OptionalInvariant(path.Precision);
            yield return OptionalInvariant(path.Scale);
            yield return path.Collation;
            yield return NullableBoolean(path.IsSingleton);
        }
        foreach (SqlServerSpatialIndexMetadata index in snapshot.SpatialIndexes
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.IndexId))
        {
            yield return "spatial-index";
            yield return Invariant(index.ObjectId);
            yield return Invariant(index.IndexId);
            yield return Invariant(index.SpatialIndexType);
            yield return index.SpatialIndexTypeDescription;
            yield return index.TessellationScheme;
        }
        foreach (SqlServerSpatialIndexTessellationMetadata tessellation in
                 snapshot.SpatialIndexTessellations
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.IndexId))
        {
            yield return "spatial-index-tessellation";
            yield return Invariant(tessellation.ObjectId);
            yield return Invariant(tessellation.IndexId);
            yield return tessellation.TessellationScheme;
            yield return OptionalInvariant(tessellation.BoundingBoxXMin);
            yield return OptionalInvariant(tessellation.BoundingBoxYMin);
            yield return OptionalInvariant(tessellation.BoundingBoxXMax);
            yield return OptionalInvariant(tessellation.BoundingBoxYMax);
            yield return OptionalInvariant(tessellation.Level1Grid);
            yield return tessellation.Level1GridDescription;
            yield return OptionalInvariant(tessellation.Level2Grid);
            yield return tessellation.Level2GridDescription;
            yield return OptionalInvariant(tessellation.Level3Grid);
            yield return tessellation.Level3GridDescription;
            yield return OptionalInvariant(tessellation.Level4Grid);
            yield return tessellation.Level4GridDescription;
            yield return OptionalInvariant(tessellation.CellsPerObject);
        }
        foreach (SqlServerHashIndexMetadata index in snapshot.HashIndexes
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.IndexId))
        {
            yield return "hash-index";
            yield return Invariant(index.ObjectId);
            yield return Invariant(index.IndexId);
            yield return Invariant(index.BucketCount);
        }
        foreach (SqlServerJsonIndexMetadata index in snapshot.JsonIndexes
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.IndexId))
        {
            yield return "json-index";
            yield return Invariant(index.ObjectId);
            yield return Invariant(index.IndexId);
            yield return Boolean(index.OptimizeForArraySearch);
        }
        foreach (SqlServerJsonIndexPathMetadata path in snapshot.JsonIndexPaths
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.IndexId)
                     .ThenBy(static item => item.PathOrdinal))
        {
            yield return "json-index-path";
            yield return Invariant(path.ObjectId);
            yield return Invariant(path.IndexId);
            yield return Invariant(path.PathOrdinal);
            yield return Invariant(path.PathBytes);
            yield return path.Path;
        }
    }
}
