using CSharpDB.Migration;

namespace CSharpDB.Migration.SqlServer;

internal static partial class SqlServerCatalogBuilder
{
    private const string PhysicalStorageRule =
        "MIG-SQLSERVER-PHYSICAL-STORAGE-NOT-LOWERED-001";
    private const string PartitioningRule =
        "MIG-SQLSERVER-PARTITIONING-UNSUPPORTED-001";
    private const string FullTextMetadataIncompleteRule =
        "MIG-SQLSERVER-FULLTEXT-METADATA-INCOMPLETE-001";

    private static int PhysicalObjectCapacity(SqlServerCatalogSnapshot snapshot) =>
        checked(
            CountIndexedViewIndexes(snapshot) +
            snapshot.FullTextCatalogs.Count +
            snapshot.FullTextStoplists.Count +
            snapshot.SearchPropertyLists.Count +
            snapshot.FullTextIndexes.Count +
            snapshot.FullTextIndexColumns.Count +
            snapshot.DataSpaces.Count +
            snapshot.PartitionFunctions.Count +
            snapshot.PartitionParameters.Count +
            snapshot.PartitionRangeValues.Count +
            snapshot.PartitionSchemes.Count +
            snapshot.PartitionSchemeDestinations.Count +
            snapshot.IndexPartitions.Count);

    private static int CountIndexedViewIndexes(SqlServerCatalogSnapshot snapshot)
    {
        HashSet<int> viewIds = snapshot.Views
            .Select(static item => item.ObjectId)
            .ToHashSet();
        return snapshot.Indexes.Count(item => viewIds.Contains(item.ObjectId));
    }

    private static string PhysicalInventoryStatus(MetadataVisibility visibility) =>
        visibility == MetadataVisibility.Complete
            ? "captured"
            : "visibility-unqualified";

    private static void AddPhysicalObjects(
        SqlServerCatalogSnapshot snapshot,
        SqlServerScriptDomAnalysisSnapshot scriptDomAnalysis,
        string databaseId,
        IReadOnlyDictionary<int, (SqlServerSchemaMetadata Metadata, string ObjectId)>
            schemasById,
        IReadOnlyDictionary<int, (SqlServerTableMetadata Metadata, string Id)>
            tablesByObjectId,
        IReadOnlyDictionary<
            (int ObjectId, int ColumnId),
            (SqlServerColumnMetadata Metadata, string Id)> tableColumnsByCatalogId,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics,
        CancellationToken cancellationToken)
    {
        Dictionary<int, PhysicalRelation> relations = BuildPhysicalRelations(
            snapshot,
            schemasById,
            tablesByObjectId);
        Dictionary<(int ObjectId, int ColumnId), PhysicalColumn> columns =
            BuildPhysicalColumns(
                snapshot,
                relations,
                tableColumnsByCatalogId);
        Dictionary<int, SqlServerDataSpaceMetadata> dataSpacesById =
            snapshot.DataSpaces.ToDictionary(static item => item.DataSpaceId);
        Dictionary<int, string> dataSpaceObjectIds = snapshot.DataSpaces
            .ToDictionary(
                static item => item.DataSpaceId,
                static item => ObjectId("data-space", item.Name));

        AddDataSpaceObjects(
            snapshot,
            databaseId,
            objects,
            diagnostics,
            cancellationToken);
        AddFullTextConfigurationObjects(
            snapshot,
            databaseId,
            dataSpaceObjectIds,
            objects,
            diagnostics,
            cancellationToken);

        Dictionary<int, string> partitionFunctionObjectIds =
            AddPartitionFunctionObjects(
                snapshot,
                databaseId,
                objects,
                diagnostics,
                cancellationToken);
        Dictionary<int, string> partitionSchemeObjectIds =
            AddPartitionSchemeObjects(
                snapshot,
                databaseId,
                dataSpacesById,
                dataSpaceObjectIds,
                partitionFunctionObjectIds,
                objects,
                diagnostics,
                cancellationToken);

        Dictionary<(int ObjectId, int IndexId), string> nativeIndexObjectIds =
            BuildTableNativeIndexObjectIds(
                snapshot,
                schemasById,
                tablesByObjectId);
        AddIndexedViewObjects(
            snapshot,
            scriptDomAnalysis,
            relations,
            columns,
            nativeIndexObjectIds,
            objects,
            diagnostics,
            cancellationToken);
        AddFullTextObjects(
            snapshot,
            relations,
            columns,
            nativeIndexObjectIds,
            dataSpaceObjectIds,
            objects,
            diagnostics,
            cancellationToken);
        AddIndexPartitionObjects(
            snapshot,
            relations,
            nativeIndexObjectIds,
            dataSpaceObjectIds,
            partitionSchemeObjectIds,
            objects,
            diagnostics,
            cancellationToken);
    }

    private static Dictionary<int, PhysicalRelation> BuildPhysicalRelations(
        SqlServerCatalogSnapshot snapshot,
        IReadOnlyDictionary<int, (SqlServerSchemaMetadata Metadata, string ObjectId)>
            schemasById,
        IReadOnlyDictionary<int, (SqlServerTableMetadata Metadata, string Id)>
            tablesByObjectId)
    {
        var result = new Dictionary<int, PhysicalRelation>();
        foreach ((int objectId, (SqlServerTableMetadata table, string tableId)) in
                 tablesByObjectId)
        {
            SqlServerSchemaMetadata schema = schemasById[table.SchemaId].Metadata;
            result.Add(
                objectId,
                new PhysicalRelation(
                    objectId,
                    schema.Name,
                    table.Name,
                    tableId,
                    IsView: false));
        }

        foreach (SqlServerViewMetadata view in snapshot.Views)
        {
            SqlServerSchemaMetadata schema = schemasById[view.SchemaId].Metadata;
            result.Add(
                view.ObjectId,
                new PhysicalRelation(
                    view.ObjectId,
                    schema.Name,
                    view.Name,
                    ObjectId("view", schema.Name, view.Name),
                    IsView: true));
        }

        return result;
    }

    private static Dictionary<(int ObjectId, int ColumnId), PhysicalColumn>
        BuildPhysicalColumns(
            SqlServerCatalogSnapshot snapshot,
            IReadOnlyDictionary<int, PhysicalRelation> relations,
            IReadOnlyDictionary<
                (int ObjectId, int ColumnId),
                (SqlServerColumnMetadata Metadata, string Id)> tableColumnsByCatalogId)
    {
        var result = new Dictionary<(int ObjectId, int ColumnId), PhysicalColumn>();
        foreach (((int objectId, int columnId), (SqlServerColumnMetadata metadata, string id))
                 in tableColumnsByCatalogId)
        {
            result.Add(
                (objectId, columnId),
                new PhysicalColumn(
                    objectId,
                    columnId,
                    metadata.Name,
                    id));
        }

        foreach (SqlServerViewColumnMetadata column in snapshot.ViewColumns)
        {
            PhysicalRelation relation = relations[column.ObjectId];
            result.Add(
                (column.ObjectId, column.ColumnId),
                new PhysicalColumn(
                    column.ObjectId,
                    column.ColumnId,
                    column.Name,
                    ObjectId(
                        "view-column",
                        relation.SchemaName,
                        relation.Name,
                        column.Name)));
        }

        return result;
    }

    private static void AddDataSpaceObjects(
        SqlServerCatalogSnapshot snapshot,
        string databaseId,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics,
        CancellationToken cancellationToken)
    {
        foreach (SqlServerDataSpaceMetadata dataSpace in snapshot.DataSpaces
                     .OrderBy(static item => item.DataSpaceId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string dataSpaceId = ObjectId("data-space", dataSpace.Name);
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = dataSpaceId,
                Kind = MigrationObjectKind.Other,
                ParentObjectId = databaseId,
                SourceName = dataSpace.Name,
                Facets =
                [
                    Facet("sqlServerObjectClass", "data-space"),
                    Facet("sqlServerDataSpaceId", Invariant(dataSpace.DataSpaceId)),
                    Facet("sqlServerDataSpaceType", dataSpace.Type),
                    Facet(
                        "sqlServerDataSpaceTypeDescription",
                        dataSpace.TypeDescription),
                    Facet("sqlServerDefault", Boolean(dataSpace.IsDefault)),
                    Facet("sqlServerSystem", Boolean(dataSpace.IsSystem)),
                    Facet(
                        "sqlServerReadOnly",
                        NullableBoolean(dataSpace.IsReadOnly)),
                ],
            });
            diagnostics.Add(Diagnostic(
                dataSpaceId,
                PhysicalStorageRule,
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "SQL Server physical storage placement is not target-compatible.",
                "The data-space or filegroup definition is inventoried as source physical metadata only. CSharpDB does not expose a compatible filegroup or data-space lowering contract.",
                "Select and validate an explicit CSharpDB storage design.",
                canOverride: false));
        }
    }

    private static void AddFullTextConfigurationObjects(
        SqlServerCatalogSnapshot snapshot,
        string databaseId,
        IReadOnlyDictionary<int, string> dataSpaceObjectIds,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics,
        CancellationToken cancellationToken)
    {
        foreach (SqlServerFullTextCatalogMetadata catalog in snapshot.FullTextCatalogs
                     .OrderBy(static item => item.FullTextCatalogId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string catalogId = ObjectId("full-text-catalog", catalog.Name);
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = catalogId,
                Kind = MigrationObjectKind.Other,
                ParentObjectId = databaseId,
                SourceName = catalog.Name,
                Facets =
                [
                    Facet("sqlServerObjectClass", "full-text-catalog"),
                    Facet(
                        "sqlServerFullTextCatalogId",
                        Invariant(catalog.FullTextCatalogId)),
                    Facet("sqlServerDefault", Boolean(catalog.IsDefault)),
                    Facet(
                        "sqlServerAccentSensitivity",
                        Boolean(catalog.IsAccentSensitivityOn)),
                    Facet(
                        "sqlServerDataSpaceId",
                        Invariant(catalog.DataSpaceId)),
                ],
                DependsOn = OptionalReference(
                    dataSpaceObjectIds,
                    catalog.DataSpaceId),
            });
        }

        foreach (SqlServerFullTextStoplistMetadata stoplist in
                 snapshot.FullTextStoplists.OrderBy(static item => item.StoplistId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = ObjectId("full-text-stoplist", stoplist.Name),
                Kind = MigrationObjectKind.Other,
                ParentObjectId = databaseId,
                SourceName = stoplist.Name,
                Facets =
                [
                    Facet("sqlServerObjectClass", "full-text-stoplist"),
                    Facet("sqlServerStoplistId", Invariant(stoplist.StoplistId)),
                    Facet("sqlServerStopwordInventory", "not-retained"),
                ],
            });
        }

        foreach (SqlServerSearchPropertyListMetadata propertyList in
                 snapshot.SearchPropertyLists
                     .OrderBy(static item => item.PropertyListId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = ObjectId(
                    "search-property-list",
                    propertyList.Name),
                Kind = MigrationObjectKind.Other,
                ParentObjectId = databaseId,
                SourceName = propertyList.Name,
                Facets =
                [
                    Facet("sqlServerObjectClass", "search-property-list"),
                    Facet(
                        "sqlServerPropertyListId",
                        Invariant(propertyList.PropertyListId)),
                    Facet("sqlServerSearchPropertyInventory", "not-retained"),
                ],
            });
        }
    }

    private static Dictionary<int, string> AddPartitionFunctionObjects(
        SqlServerCatalogSnapshot snapshot,
        string databaseId,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics,
        CancellationToken cancellationToken)
    {
        var parameters = new Dictionary<
            (int FunctionId, int ParameterId),
            SqlServerPartitionParameterMetadata>();
        var parametersByFunction = new Dictionary<
            int,
            SortedDictionary<int, SqlServerPartitionParameterMetadata>>();
        foreach (SqlServerPartitionParameterMetadata parameter in
                 snapshot.PartitionParameters)
        {
            cancellationToken.ThrowIfCancellationRequested();
            parameters.Add(
                (parameter.FunctionId, parameter.ParameterId),
                parameter);
            if (!parametersByFunction.TryGetValue(
                    parameter.FunctionId,
                    out SortedDictionary<
                        int,
                        SqlServerPartitionParameterMetadata>? ownedParameters))
            {
                ownedParameters = [];
                parametersByFunction.Add(parameter.FunctionId, ownedParameters);
            }
            ownedParameters.Add(parameter.ParameterId, parameter);
        }

        var boundariesByFunction = new Dictionary<
            int,
            SortedDictionary<
                (int BoundaryId, int ParameterId),
                SqlServerPartitionRangeValueMetadata>>();
        foreach (SqlServerPartitionRangeValueMetadata boundary in
                 snapshot.PartitionRangeValues)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!boundariesByFunction.TryGetValue(
                    boundary.FunctionId,
                    out SortedDictionary<
                        (int BoundaryId, int ParameterId),
                        SqlServerPartitionRangeValueMetadata>? ownedBoundaries))
            {
                ownedBoundaries = [];
                boundariesByFunction.Add(boundary.FunctionId, ownedBoundaries);
            }
            ownedBoundaries.Add(
                (boundary.BoundaryId, boundary.ParameterId),
                boundary);
        }

        Dictionary<int, string> functionObjectIds = snapshot.PartitionFunctions
            .ToDictionary(
                static item => item.FunctionId,
                static item => ObjectId("partition-function", item.Name));

        foreach (SqlServerPartitionFunctionMetadata function in
                 snapshot.PartitionFunctions.OrderBy(static item => item.FunctionId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string functionId = functionObjectIds[function.FunctionId];
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = functionId,
                Kind = MigrationObjectKind.Other,
                ParentObjectId = databaseId,
                SourceName = function.Name,
                Facets =
                [
                    Facet("sqlServerObjectClass", "partition-function"),
                    Facet(
                        "sqlServerPartitionFunctionId",
                        Invariant(function.FunctionId)),
                    Facet("sqlServerFanout", Invariant(function.Fanout)),
                    Facet(
                        "sqlServerBoundaryValueOnRight",
                        Boolean(function.BoundaryValueOnRight)),
                    Facet("sqlServerSystem", Boolean(function.IsSystem)),
                ],
            });
            diagnostics.Add(PartitionDiagnostic(
                functionId,
                "The SQL Server partition function is not target-compatible."));

            IEnumerable<SqlServerPartitionParameterMetadata> functionParameters =
                parametersByFunction.TryGetValue(
                    function.FunctionId,
                    out SortedDictionary<
                        int,
                        SqlServerPartitionParameterMetadata>? ownedParameters)
                    ? ownedParameters.Values
                    : [];
            foreach (SqlServerPartitionParameterMetadata parameter in
                     functionParameters)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string parameterId = PartitionParameterObjectId(
                    function,
                    parameter.ParameterId);
                objects.Add(new MigrationCatalogObject
                {
                    ObjectId = parameterId,
                    Kind = MigrationObjectKind.Other,
                    ParentObjectId = functionId,
                    SourceName = "$parameter-" +
                        Invariant(parameter.ParameterId),
                    NativeType = FormatPartitionParameterNativeType(parameter),
                    Facets =
                    [
                        Facet("sqlServerObjectClass", "partition-parameter"),
                        Facet(
                            "sqlServerParameterId",
                            Invariant(parameter.ParameterId)),
                        Facet("sqlServerTypeSchema", parameter.TypeSchema),
                        Facet("sqlServerTypeName", parameter.TypeName),
                        Facet(
                            "sqlServerSystemTypeName",
                            parameter.SystemTypeName),
                        Facet(
                            "sqlServerMaxLengthBytes",
                            Invariant(parameter.MaxLength)),
                        Facet(
                            "sqlServerPrecision",
                            Invariant(parameter.Precision)),
                        Facet("sqlServerScale", Invariant(parameter.Scale)),
                        Facet("sqlServerCollation", parameter.Collation),
                    ],
                });
            }

            IEnumerable<SqlServerPartitionRangeValueMetadata> functionBoundaries =
                boundariesByFunction.TryGetValue(
                    function.FunctionId,
                    out SortedDictionary<
                        (int BoundaryId, int ParameterId),
                        SqlServerPartitionRangeValueMetadata>? ownedBoundaries)
                    ? ownedBoundaries.Values
                    : [];
            foreach (SqlServerPartitionRangeValueMetadata boundary in
                     functionBoundaries)
            {
                cancellationToken.ThrowIfCancellationRequested();
                SqlServerPartitionParameterMetadata parameter =
                    parameters[(boundary.FunctionId, boundary.ParameterId)];
                string boundaryId = ObjectId(
                    "partition-boundary",
                    function.Name,
                    Invariant(boundary.BoundaryId),
                    Invariant(boundary.ParameterId));
                objects.Add(new MigrationCatalogObject
                {
                    ObjectId = boundaryId,
                    Kind = MigrationObjectKind.Other,
                    ParentObjectId = functionId,
                    SourceName = "$boundary-" +
                        Invariant(boundary.BoundaryId),
                    Facets =
                    [
                        Facet("sqlServerObjectClass", "partition-boundary"),
                        Facet(
                            "sqlServerBoundaryId",
                            Invariant(boundary.BoundaryId)),
                        Facet(
                            "sqlServerParameterId",
                            Invariant(boundary.ParameterId)),
                        Facet(
                            "sqlServerBoundaryNull",
                            Boolean(boundary.IsNull)),
                        Facet("sqlServerBoundaryBaseType", boundary.BaseType),
                        Facet(
                            "sqlServerBoundaryMaxLength",
                            OptionalInvariant(boundary.MaxLength)),
                        Facet(
                            "sqlServerBoundaryPrecision",
                            OptionalInvariant(boundary.Precision)),
                        Facet(
                            "sqlServerBoundaryScale",
                            OptionalInvariant(boundary.Scale)),
                        Facet(
                            "sqlServerBoundaryCollation",
                            boundary.Collation),
                        Facet(
                            "sqlServerBoundaryValueBytes",
                            OptionalInvariant(boundary.ValueBytes)),
                        Facet(
                            "sqlServerBoundaryValueDigest",
                            PartitionBoundaryDigest(boundary)),
                    ],
                    DependsOn =
                    [
                        PartitionParameterObjectId(
                            function,
                            parameter.ParameterId),
                    ],
                });
            }
        }

        return functionObjectIds;
    }

    private static Dictionary<int, string> AddPartitionSchemeObjects(
        SqlServerCatalogSnapshot snapshot,
        string databaseId,
        IReadOnlyDictionary<int, SqlServerDataSpaceMetadata> dataSpacesById,
        IReadOnlyDictionary<int, string> dataSpaceObjectIds,
        IReadOnlyDictionary<int, string> partitionFunctionObjectIds,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics,
        CancellationToken cancellationToken)
    {
        var destinationsByScheme = new Dictionary<
            int,
            SortedDictionary<
                int,
                SqlServerPartitionSchemeDestinationMetadata>>();
        foreach (SqlServerPartitionSchemeDestinationMetadata destination in
                 snapshot.PartitionSchemeDestinations)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!destinationsByScheme.TryGetValue(
                    destination.PartitionSchemeId,
                    out SortedDictionary<
                        int,
                        SqlServerPartitionSchemeDestinationMetadata>?
                        ownedDestinations))
            {
                ownedDestinations = [];
                destinationsByScheme.Add(
                    destination.PartitionSchemeId,
                    ownedDestinations);
            }
            ownedDestinations.Add(destination.DestinationId, destination);
        }

        var schemeObjectIds = new Dictionary<int, string>();
        foreach (SqlServerPartitionSchemeMetadata scheme in snapshot.PartitionSchemes
                     .OrderBy(static item => item.DataSpaceId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            SqlServerDataSpaceMetadata dataSpace = dataSpacesById[scheme.DataSpaceId];
            string schemeId = ObjectId("partition-scheme", dataSpace.Name);
            schemeObjectIds.Add(scheme.DataSpaceId, schemeId);
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = schemeId,
                Kind = MigrationObjectKind.Other,
                ParentObjectId = databaseId,
                SourceName = dataSpace.Name,
                Facets =
                [
                    Facet("sqlServerObjectClass", "partition-scheme"),
                    Facet(
                        "sqlServerPartitionSchemeId",
                        Invariant(scheme.DataSpaceId)),
                    Facet(
                        "sqlServerPartitionFunctionId",
                        Invariant(scheme.FunctionId)),
                ],
                DependsOn =
                [
                    dataSpaceObjectIds[scheme.DataSpaceId],
                    partitionFunctionObjectIds[scheme.FunctionId],
                ],
            });
            diagnostics.Add(PartitionDiagnostic(
                schemeId,
                "The SQL Server partition scheme is not target-compatible."));

            IEnumerable<SqlServerPartitionSchemeDestinationMetadata>
                schemeDestinations = destinationsByScheme.TryGetValue(
                    scheme.DataSpaceId,
                    out SortedDictionary<
                        int,
                        SqlServerPartitionSchemeDestinationMetadata>?
                        ownedDestinations)
                    ? ownedDestinations.Values
                    : [];
            foreach (SqlServerPartitionSchemeDestinationMetadata destination in
                     schemeDestinations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                objects.Add(new MigrationCatalogObject
                {
                    ObjectId = ObjectId(
                        "partition-destination",
                        dataSpace.Name,
                        Invariant(destination.DestinationId)),
                    Kind = MigrationObjectKind.Other,
                    ParentObjectId = schemeId,
                    SourceName = "$destination-" +
                        Invariant(destination.DestinationId),
                    Facets =
                    [
                        Facet(
                            "sqlServerObjectClass",
                            "partition-scheme-destination"),
                        Facet(
                            "sqlServerDestinationId",
                            Invariant(destination.DestinationId)),
                        Facet(
                            "sqlServerDataSpaceId",
                            Invariant(destination.DataSpaceId)),
                    ],
                    DependsOn =
                    [
                        dataSpaceObjectIds[destination.DataSpaceId],
                    ],
                });
            }
        }

        return schemeObjectIds;
    }

    private static Dictionary<(int ObjectId, int IndexId), string>
        BuildTableNativeIndexObjectIds(
            SqlServerCatalogSnapshot snapshot,
            IReadOnlyDictionary<int, (SqlServerSchemaMetadata Metadata, string ObjectId)>
                schemasById,
            IReadOnlyDictionary<int, (SqlServerTableMetadata Metadata, string Id)>
                tablesByObjectId)
    {
        var result = new Dictionary<(int ObjectId, int IndexId), string>();
        foreach (SqlServerKeyMetadata key in snapshot.Keys)
        {
            SqlServerTableMetadata table = tablesByObjectId[key.ParentObjectId].Metadata;
            SqlServerSchemaMetadata schema = schemasById[table.SchemaId].Metadata;
            result.Add(
                (key.ParentObjectId, key.UniqueIndexId),
                ObjectId("key", schema.Name, table.Name, key.Name));
        }

        HashSet<(int ObjectId, int IndexId)> keyIndexes = result.Keys.ToHashSet();
        foreach (SqlServerIndexMetadata index in snapshot.Indexes)
        {
            if (!tablesByObjectId.TryGetValue(
                    index.ObjectId,
                    out (SqlServerTableMetadata Metadata, string Id) tableEntry) ||
                keyIndexes.Contains((index.ObjectId, index.IndexId)))
            {
                continue;
            }

            SqlServerSchemaMetadata schema =
                schemasById[tableEntry.Metadata.SchemaId].Metadata;
            result.Add(
                (index.ObjectId, index.IndexId),
                ObjectId(
                    "index",
                    schema.Name,
                    tableEntry.Metadata.Name,
                    index.Name));
        }

        return result;
    }

    private static void AddIndexedViewObjects(
        SqlServerCatalogSnapshot snapshot,
        SqlServerScriptDomAnalysisSnapshot scriptDomAnalysis,
        IReadOnlyDictionary<int, PhysicalRelation> relations,
        IReadOnlyDictionary<(int ObjectId, int ColumnId), PhysicalColumn> columns,
        IDictionary<(int ObjectId, int IndexId), string> nativeIndexObjectIds,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics,
        CancellationToken cancellationToken)
    {
        Dictionary<(int ObjectId, int IndexId), SqlServerIndexColumnMetadata[]>
            indexColumns = snapshot.IndexColumns
                .GroupBy(static item => (item.ObjectId, item.IndexId))
                .ToDictionary(
                    static group => group.Key,
                    static group => group
                        .OrderBy(static item => item.IndexColumnId)
                        .ToArray());

        foreach (SqlServerIndexMetadata index in snapshot.Indexes
                     .Where(item =>
                         relations.TryGetValue(
                             item.ObjectId,
                             out PhysicalRelation? relation) &&
                         relation.IsView)
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.IndexId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            PhysicalRelation relation = relations[index.ObjectId];
            indexColumns.TryGetValue(
                (index.ObjectId, index.IndexId),
                out SqlServerIndexColumnMetadata[]? allColumns);
            allColumns ??= [];
            SqlServerIndexColumnMetadata[] keyColumns = allColumns
                .Where(static item => item.KeyOrdinal > 0 && !item.IsIncluded)
                .OrderBy(static item => item.KeyOrdinal)
                .ThenBy(static item => item.IndexColumnId)
                .ToArray();
            bool completeMembership =
                keyColumns.Length > 0 &&
                HasContiguousKeyOrdinals(keyColumns) &&
                keyColumns
                    .Select(static item => item.ColumnId)
                    .Distinct()
                    .Count() == keyColumns.Length &&
                allColumns.All(item =>
                    item.ColumnId > 0 &&
                    columns.ContainsKey((item.ObjectId, item.ColumnId)));
            bool hasIncludedColumns =
                allColumns.Any(static item => item.IsIncluded);
            bool hasDescendingKeys =
                keyColumns.Any(static item => item.IsDescending);
            string indexId = ObjectId(
                "index",
                relation.SchemaName,
                relation.Name,
                index.Name);
            nativeIndexObjectIds.Add((index.ObjectId, index.IndexId), indexId);

            SqlServerScriptDomDefinitionAnalysis? filterAnalysis =
                GetScriptDomAnalysis(
                    scriptDomAnalysis,
                    SqlServerScriptDomDefinitionKind.IndexFilterPredicate,
                    index.ObjectId,
                    index.IndexId,
                    index.FilterDefinition is not null);
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("kind", "sqlserver-indexed-view-index"),
                Facet("unique", Boolean(index.IsUnique)),
                Facet("sqlServerIndexedView", "true"),
                Facet(
                    "sqlServerMembershipComplete",
                    Boolean(completeMembership)),
            };
            if (index.HasFilter)
                facets.Add(Facet("partial", "true"));
            if (hasIncludedColumns)
                facets.Add(Facet("includedColumns", "true"));
            if (hasDescendingKeys)
                facets.Add(Facet("sortDirections", "descending"));
            AddPhysicalIndexFacets(facets, index, allColumns);
            AddDefinitionDigestFacets(
                facets,
                "sqlServerFilterDefinition",
                "csharpdb-sqlserver-filter-definition/v1",
                index.FilterDefinitionBytes,
                index.FilterDefinition);
            if (index.HasFilter)
            {
                AddScriptDomFacets(
                    facets,
                    "sqlServerFilterTsqlAnalysis",
                    "sqlServerFilterTsql",
                    filterAnalysis);
            }

            (SqlServerIndexColumnMetadata Metadata, PhysicalColumn Column)[]
                resolvedKeyColumns = keyColumns
                    .Where(item => columns.ContainsKey(
                        (item.ObjectId, item.ColumnId)))
                    .Select(item => (
                        item,
                        columns[(item.ObjectId, item.ColumnId)]))
                    .ToArray();
            string[] dependencies = allColumns
                .Where(item => columns.ContainsKey(
                    (item.ObjectId, item.ColumnId)))
                .OrderBy(static item => item.IndexColumnId)
                .Select(item => columns[(item.ObjectId, item.ColumnId)].Id)
                .Distinct(StringComparer.Ordinal)
                .ToArray();
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = indexId,
                Kind = MigrationObjectKind.Index,
                ParentObjectId = relation.Id,
                SourceNamespace = relation.SchemaName,
                SourceName = index.Name,
                Facets = facets.AsReadOnly(),
                Members = resolvedKeyColumns
                    .Select((item, ordinal) => Member(
                        item.Column.Id,
                        MigrationObjectReferenceRoles.Column,
                        ordinal))
                    .ToArray(),
                DependsOn = dependencies,
            });
            diagnostics.Add(Diagnostic(
                indexId,
                "MIG-SQLSERVER-INDEXED-VIEW-INDEX-UNSUPPORTED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "The SQL Server indexed-view index is not target-compatible.",
                "The physical index and ordered view-column membership are inventoried, but CSharpDB does not advertise indexed-view maintenance or an equivalent target index contract.",
                "Materialize and validate an ordinary target table or provide a reviewed target design.",
                canOverride: false));
            if (!completeMembership)
            {
                diagnostics.Add(Diagnostic(
                    indexId,
                    "MIG-SQLSERVER-INDEXED-VIEW-MEMBERSHIP-UNKNOWN-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unknown,
                    "The indexed-view index membership could not be proven.",
                    "One or more ordered index-column references were absent or invalid in the bounded view-column inventory.",
                    "Restore complete metadata visibility and inspect again.",
                    canOverride: false));
            }
            AddFilterAnalysisDiagnostic(
                indexId,
                index,
                filterAnalysis,
                diagnostics);
        }
    }

    private static void AddFullTextObjects(
        SqlServerCatalogSnapshot snapshot,
        IReadOnlyDictionary<int, PhysicalRelation> relations,
        IReadOnlyDictionary<(int ObjectId, int ColumnId), PhysicalColumn> columns,
        IReadOnlyDictionary<(int ObjectId, int IndexId), string>
            nativeIndexObjectIds,
        IReadOnlyDictionary<int, string> dataSpaceObjectIds,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics,
        CancellationToken cancellationToken)
    {
        Dictionary<int, SqlServerFullTextCatalogMetadata> catalogs =
            snapshot.FullTextCatalogs.ToDictionary(
                static item => item.FullTextCatalogId);
        Dictionary<int, SqlServerFullTextStoplistMetadata> stoplists =
            snapshot.FullTextStoplists.ToDictionary(static item => item.StoplistId);
        Dictionary<int, SqlServerSearchPropertyListMetadata> propertyLists =
            snapshot.SearchPropertyLists.ToDictionary(
                static item => item.PropertyListId);
        Dictionary<int, SqlServerFullTextIndexColumnMetadata[]> columnsByIndex =
            snapshot.FullTextIndexColumns
                .GroupBy(static item => item.ObjectId)
                .ToDictionary(
                    static group => group.Key,
                    static group => group
                        .OrderBy(static item => item.ColumnId)
                        .ToArray());

        foreach (SqlServerFullTextIndexMetadata index in snapshot.FullTextIndexes
                     .OrderBy(static item => item.ObjectId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            PhysicalRelation relation = relations[index.ObjectId];
            bool hasCatalog = catalogs.TryGetValue(
                index.FullTextCatalogId,
                out SqlServerFullTextCatalogMetadata? catalog);
            SqlServerFullTextStoplistMetadata? stoplist = null;
            bool hasStoplist = index.StoplistId is not > 0 ||
                stoplists.TryGetValue(
                    index.StoplistId.Value,
                    out stoplist);
            SqlServerSearchPropertyListMetadata? propertyList = null;
            bool hasPropertyList = index.PropertyListId is not > 0 ||
                propertyLists.TryGetValue(
                    index.PropertyListId.Value,
                    out propertyList);
            columnsByIndex.TryGetValue(
                index.ObjectId,
                out SqlServerFullTextIndexColumnMetadata[]? indexColumns);
            indexColumns ??= [];
            string indexId = ObjectId(
                "full-text-index",
                relation.SchemaName,
                relation.Name);
            var dependencies = new List<string>
            {
                nativeIndexObjectIds[(index.ObjectId, index.UniqueIndexId)],
            };
            if (hasCatalog)
            {
                dependencies.Add(ObjectId(
                    "full-text-catalog",
                    catalog!.Name));
            }
            if (index.StoplistId is > 0 && hasStoplist)
            {
                dependencies.Add(ObjectId(
                    "full-text-stoplist",
                    stoplist!.Name));
            }
            if (index.PropertyListId is > 0 && hasPropertyList)
            {
                dependencies.Add(ObjectId(
                    "search-property-list",
                    propertyList!.Name));
            }
            if (index.DataSpaceId > 0)
                dependencies.Add(dataSpaceObjectIds[index.DataSpaceId]);
            foreach (SqlServerFullTextIndexColumnMetadata column in indexColumns)
            {
                dependencies.Add(columns[(column.ObjectId, column.ColumnId)].Id);
                if (column.TypeColumnId is int typeColumnId)
                {
                    dependencies.Add(columns[(
                        column.ObjectId,
                        typeColumnId)].Id);
                }
            }

            objects.Add(new MigrationCatalogObject
            {
                ObjectId = indexId,
                Kind = MigrationObjectKind.Index,
                ParentObjectId = relation.Id,
                SourceNamespace = relation.SchemaName,
                SourceName = "$fulltext",
                Facets =
                [
                    Facet("kind", "sqlserver-full-text-index"),
                    Facet("unique", "false"),
                    Facet("sqlServerObjectClass", "full-text-index"),
                    Facet("sqlServerObjectId", Invariant(index.ObjectId)),
                    Facet(
                        "sqlServerUniqueIndexId",
                        Invariant(index.UniqueIndexId)),
                    Facet(
                        "sqlServerIndexVersion",
                        OptionalInvariant(index.IndexVersion)),
                    Facet(
                        "sqlServerFullTextCatalogId",
                        Invariant(index.FullTextCatalogId)),
                    Facet("sqlServerEnabled", Boolean(index.IsEnabled)),
                    Facet(
                        "sqlServerChangeTrackingState",
                        index.ChangeTrackingState),
                    Facet(
                        "sqlServerChangeTrackingStateDescription",
                        index.ChangeTrackingStateDescription),
                    Facet(
                        "sqlServerStoplistId",
                        OptionalInvariant(index.StoplistId)),
                    Facet(
                        "sqlServerStoplistMode",
                        index.StoplistId switch
                        {
                            null => "off",
                            0 => "system",
                            _ => "custom",
                        }),
                    Facet(
                        "sqlServerDataSpaceId",
                        Invariant(index.DataSpaceId)),
                    Facet(
                        "sqlServerPropertyListId",
                        OptionalInvariant(index.PropertyListId)),
                ],
                Members = indexColumns
                    .Select((item, ordinal) => Member(
                        columns[(item.ObjectId, item.ColumnId)].Id,
                        MigrationObjectReferenceRoles.Column,
                        ordinal))
                    .ToArray(),
                DependsOn = dependencies
                    .Distinct(StringComparer.Ordinal)
                    .OrderBy(static item => item, StringComparer.Ordinal)
                    .ToArray(),
            });
            diagnostics.Add(Diagnostic(
                indexId,
                "MIG-SQLSERVER-FULLTEXT-INDEX-UNSUPPORTED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "The SQL Server full-text index is not target-compatible.",
                "Language, tokenizer, stoplist, type-column, property-list, change-tracking, and statistical-semantics behavior is inventoried without claiming equivalence to the CSharpDB full-text API or a target SQL DDL form.",
                "Choose and differentially validate an explicit target full-text design.",
                canOverride: false));
            if (!hasCatalog || !hasStoplist || !hasPropertyList)
            {
                diagnostics.Add(Diagnostic(
                    indexId,
                    FullTextMetadataIncompleteRule,
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unknown,
                    "The SQL Server full-text configuration metadata is incomplete.",
                    "The visible full-text index retained its native catalog, custom stoplist, and property-list identifiers, but one or more referenced configuration rows were hidden from this restricted inspection.",
                    "Restore complete metadata visibility and inspect again.",
                    canOverride: false));
            }

            foreach ((SqlServerFullTextIndexColumnMetadata column, int ordinal) in
                     indexColumns.Select((item, ordinal) => (item, ordinal)))
            {
                PhysicalColumn sourceColumn =
                    columns[(column.ObjectId, column.ColumnId)];
                var columnDependencies = new List<string> { sourceColumn.Id };
                if (column.TypeColumnId is int typeColumnId)
                {
                    columnDependencies.Add(columns[(
                        column.ObjectId,
                        typeColumnId)].Id);
                }
                objects.Add(new MigrationCatalogObject
                {
                    ObjectId = ObjectId(
                        "full-text-index-column",
                        relation.SchemaName,
                        relation.Name,
                        sourceColumn.Name),
                    Kind = MigrationObjectKind.Other,
                    ParentObjectId = indexId,
                    SourceNamespace = relation.SchemaName,
                    SourceName = sourceColumn.Name,
                    Facets =
                    [
                        Facet(
                            "sqlServerObjectClass",
                            "full-text-index-column"),
                        Facet(
                            "sqlServerMembershipOrdinal",
                            Invariant(ordinal)),
                        Facet(
                            "sqlServerColumnId",
                            Invariant(column.ColumnId)),
                        Facet(
                            "sqlServerTypeColumnId",
                            OptionalInvariant(column.TypeColumnId)),
                        Facet(
                            "sqlServerLanguageId",
                            Invariant(column.LanguageId)),
                        Facet(
                            "sqlServerStatisticalSemantics",
                            Boolean(column.StatisticalSemantics)),
                    ],
                    DependsOn = columnDependencies.ToArray(),
                });
            }
        }
    }

    private static void AddIndexPartitionObjects(
        SqlServerCatalogSnapshot snapshot,
        IReadOnlyDictionary<int, PhysicalRelation> relations,
        IReadOnlyDictionary<(int ObjectId, int IndexId), string>
            nativeIndexObjectIds,
        IReadOnlyDictionary<int, string> dataSpaceObjectIds,
        IReadOnlyDictionary<int, string> partitionSchemeObjectIds,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics,
        CancellationToken cancellationToken)
    {
        var diagnosedOwners = new HashSet<(int ObjectId, int IndexId)>();
        foreach (SqlServerIndexPartitionMetadata partition in snapshot.IndexPartitions
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.IndexId)
                     .ThenBy(static item => item.PartitionNumber))
        {
            cancellationToken.ThrowIfCancellationRequested();
            PhysicalRelation relation = relations[partition.ObjectId];
            string parentId = partition.IndexId == 0
                ? relation.Id
                : nativeIndexObjectIds[(partition.ObjectId, partition.IndexId)];
            var dependencies = new List<string>();
            if (partition.StorageDataSpaceId is int storageDataSpaceId)
                dependencies.Add(dataSpaceObjectIds[storageDataSpaceId]);
            if (partition.DefinitionDataSpaceId is int definitionDataSpaceId &&
                partitionSchemeObjectIds.TryGetValue(
                    definitionDataSpaceId,
                    out string? partitionSchemeId))
            {
                dependencies.Add(partitionSchemeId);
            }

            string partitionId = ObjectId(
                "physical-partition",
                relation.SchemaName,
                relation.Name,
                Invariant(partition.IndexId),
                Invariant(partition.PartitionNumber));
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = partitionId,
                Kind = MigrationObjectKind.Other,
                ParentObjectId = parentId,
                SourceNamespace = relation.SchemaName,
                SourceName = "$partition-" +
                    Invariant(partition.PartitionNumber),
                Facets =
                [
                    Facet("sqlServerObjectClass", "physical-partition"),
                    Facet("sqlServerObjectId", Invariant(partition.ObjectId)),
                    Facet("sqlServerIndexId", Invariant(partition.IndexId)),
                    Facet(
                        "sqlServerPartitionNumber",
                        Invariant(partition.PartitionNumber)),
                    Facet(
                        "sqlServerDataCompression",
                        Invariant(partition.DataCompression)),
                    Facet(
                        "sqlServerDataCompressionDescription",
                        partition.DataCompressionDescription),
                    Facet(
                        "sqlServerXmlCompression",
                        NullableBoolean(partition.XmlCompression)),
                    Facet(
                        "sqlServerXmlCompressionDescription",
                        partition.XmlCompressionDescription),
                    Facet(
                        "sqlServerDefinitionDataSpaceId",
                        OptionalInvariant(partition.DefinitionDataSpaceId)),
                    Facet(
                        "sqlServerStorageDataSpaceId",
                        OptionalInvariant(partition.StorageDataSpaceId)),
                ],
                DependsOn = dependencies
                    .Distinct(StringComparer.Ordinal)
                    .ToArray(),
            });

            bool specialPhysicalShape =
                partition.PartitionNumber > 1 ||
                partition.DataCompression != 0 ||
                partition.XmlCompression == true ||
                partition.DefinitionDataSpaceId is int
                    specialDefinitionDataSpaceId &&
                partitionSchemeObjectIds.ContainsKey(
                    specialDefinitionDataSpaceId);
            if (specialPhysicalShape &&
                diagnosedOwners.Add((partition.ObjectId, partition.IndexId)))
            {
                diagnostics.Add(Diagnostic(
                    partitionId,
                    PhysicalStorageRule,
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unsupported,
                    "The SQL Server physical partition layout is not target-compatible.",
                    "Partition placement and compression are retained as source physical facts without a CSharpDB lowering contract.",
                    "Choose and validate an explicit CSharpDB storage design.",
                    canOverride: false));
            }
        }
    }

    private static void ValidatePhysicalCounts(
        SqlServerCatalogSnapshot snapshot,
        SqlServerInspectionLimits limits)
    {
        if (snapshot.FullTextCatalogs.Count > limits.MaxFullTextCatalogs)
            throw LimitExceeded("full-text catalog count");
        if (snapshot.FullTextStoplists.Count > limits.MaxFullTextStoplists)
            throw LimitExceeded("full-text stoplist count");
        if (snapshot.SearchPropertyLists.Count > limits.MaxSearchPropertyLists)
            throw LimitExceeded("search-property-list count");
        if (snapshot.FullTextIndexes.Count > limits.MaxFullTextIndexes)
            throw LimitExceeded("full-text index count");
        if (snapshot.FullTextIndexColumns.Count > limits.MaxFullTextIndexColumns)
            throw LimitExceeded("full-text index-column count");
        if (snapshot.DataSpaces.Count > limits.MaxDataSpaces)
            throw LimitExceeded("data-space count");
        if (snapshot.PartitionSchemes.Count > limits.MaxPartitionSchemes)
            throw LimitExceeded("partition-scheme count");
        if (snapshot.PartitionSchemeDestinations.Count >
            limits.MaxPartitionSchemeDestinations)
        {
            throw LimitExceeded("partition-scheme destination count");
        }
        if (snapshot.PartitionFunctions.Count > limits.MaxPartitionFunctions)
            throw LimitExceeded("partition-function count");
        if (snapshot.PartitionParameters.Count > limits.MaxPartitionParameters)
            throw LimitExceeded("partition-parameter count");
        if (snapshot.PartitionRangeValues.Count > limits.MaxPartitionRangeValues)
            throw LimitExceeded("partition range-value count");
        if (snapshot.IndexPartitions.Count > limits.MaxIndexPartitions)
            throw LimitExceeded("index-partition count");
    }

    private static void ValidateAggregateStructuralCount(
        SqlServerCatalogSnapshot snapshot,
        SqlServerInspectionLimits limits)
    {
        long structuralRows = checked(
            (long)snapshot.Keys.Count +
            snapshot.Indexes.Count +
            snapshot.IndexColumns.Count +
            snapshot.ForeignKeys.Count +
            snapshot.ForeignKeyColumns.Count +
            snapshot.Checks.Count +
            snapshot.Sequences.Count +
            snapshot.Views.Count +
            snapshot.ViewColumns.Count +
            snapshot.Triggers.Count +
            snapshot.TriggerEvents.Count +
            snapshot.Routines.Count +
            snapshot.Modules.Count +
            snapshot.Parameters.Count +
            snapshot.ExpressionDependencyAudit.Dependencies.Count +
            snapshot.FullTextCatalogs.Count +
            snapshot.FullTextStoplists.Count +
            snapshot.SearchPropertyLists.Count +
            snapshot.FullTextIndexes.Count +
            snapshot.FullTextIndexColumns.Count +
            snapshot.DataSpaces.Count +
            snapshot.PartitionSchemes.Count +
            snapshot.PartitionSchemeDestinations.Count +
            snapshot.PartitionFunctions.Count +
            snapshot.PartitionParameters.Count +
            snapshot.PartitionRangeValues.Count +
            snapshot.IndexPartitions.Count);
        if (structuralRows > limits.MaxStructuralRowsTotal)
            throw LimitExceeded("aggregate structural-row count");
    }

    private static void ValidatePhysicalSnapshot(
        SqlServerCatalogSnapshot snapshot,
        IReadOnlySet<int> schemaIds,
        IReadOnlySet<int> tableIds,
        IReadOnlySet<(int ObjectId, int ColumnId)> tableColumnIds,
        MetadataBudget budget,
        SqlServerInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        Dictionary<int, SqlServerViewMetadata> views = snapshot.Views
            .ToDictionary(static item => item.ObjectId);
        HashSet<int> viewIds = views.Keys.ToHashSet();
        HashSet<(int ObjectId, int ColumnId)> viewColumnIds = snapshot.ViewColumns
            .Select(static item => (item.ObjectId, item.ColumnId))
            .ToHashSet();
        HashSet<(int ObjectId, int ColumnId)> allColumnIds =
            tableColumnIds.Concat(viewColumnIds).ToHashSet();
        HashSet<int> allRelationIds = tableIds.Concat(viewIds).ToHashSet();
        if (snapshot.Indexes.Any(index =>
                !allRelationIds.Contains(index.ObjectId)) ||
            snapshot.IndexColumns.Any(column =>
                !allRelationIds.Contains(column.ObjectId)))
        {
            throw new SqlServerMigrationException(
                "SQL Server returned index metadata without a visible table or view owner.");
        }

        ValidateViewIndexes(
            snapshot,
            views,
            viewColumnIds,
            budget,
            cancellationToken);

        var dataSpaces = new Dictionary<int, SqlServerDataSpaceMetadata>();
        var dataSpaceNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (SqlServerDataSpaceMetadata dataSpace in snapshot.DataSpaces)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (dataSpace.DataSpaceId <= 0 ||
                !dataSpaces.TryAdd(dataSpace.DataSpaceId, dataSpace) ||
                !dataSpaceNames.Add(dataSpace.Name))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate or invalid data-space metadata.");
            }
            budget.Add(dataSpace.Name, isName: true);
            budget.Add(dataSpace.Type);
            budget.Add(dataSpace.TypeDescription);
        }

        foreach (SqlServerTableMetadata table in snapshot.Tables)
        {
            ValidateOptionalDataSpaceReference(
                table.LobDataSpaceId,
                dataSpaces,
                "table LOB");
            ValidateOptionalDataSpaceReference(
                table.FileStreamDataSpaceId,
                dataSpaces,
                "table FILESTREAM");
        }
        foreach (SqlServerIndexMetadata index in snapshot.Indexes)
        {
            ValidateOptionalDataSpaceReference(
                index.DataSpaceId,
                dataSpaces,
                "index");
        }

        ValidateFullTextSnapshot(
            snapshot,
            allRelationIds,
            allColumnIds,
            dataSpaces,
            budget,
            cancellationToken);
        ValidatePartitionSnapshot(
            snapshot,
            allRelationIds,
            tableIds,
            dataSpaces,
            budget,
            limits,
            cancellationToken);
    }

    private static void ValidateViewIndexes(
        SqlServerCatalogSnapshot snapshot,
        IReadOnlyDictionary<int, SqlServerViewMetadata> views,
        IReadOnlySet<(int ObjectId, int ColumnId)> viewColumnIds,
        MetadataBudget budget,
        CancellationToken cancellationToken)
    {
        var indexes =
            new Dictionary<(int ObjectId, int IndexId), SqlServerIndexMetadata>();
        foreach (SqlServerIndexMetadata index in snapshot.Indexes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!views.ContainsKey(index.ObjectId))
                continue;
            if (index.IndexId <= 0 ||
                index.Type == 0 ||
                !indexes.TryAdd((index.ObjectId, index.IndexId), index))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate or invalid indexed-view metadata.");
            }
            if (index.IsPrimaryKey || index.IsUniqueConstraint)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned constraint ownership for an indexed-view index.");
            }
            budget.Add(index.Name, isName: true);
            budget.Add(index.TypeDescription);
            budget.Add(index.DataSpaceName, isName: true);
            budget.Add(index.DataSpaceType);
            budget.ReserveExpression(index.FilterDefinitionBytes);
            budget.AddExpression(index.FilterDefinition);
            ValidateDefinitionLength(
                index.FilterDefinition,
                index.FilterDefinitionBytes,
                "indexed-view filter");
            if (!index.HasFilter &&
                (index.FilterDefinitionBytes is not null ||
                 index.FilterDefinition is not null))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned inconsistent indexed-view filter metadata.");
            }
        }

        var indexColumns = new HashSet<(
            int ObjectId,
            int IndexId,
            int IndexColumnId)>();
        foreach (SqlServerIndexColumnMetadata column in snapshot.IndexColumns)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!views.ContainsKey(column.ObjectId))
                continue;
            if (!indexes.ContainsKey((column.ObjectId, column.IndexId)) ||
                column.IndexColumnId <= 0 ||
                column.ColumnId <= 0 ||
                !viewColumnIds.Contains((column.ObjectId, column.ColumnId)) ||
                !indexColumns.Add((
                    column.ObjectId,
                    column.IndexId,
                    column.IndexColumnId)))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned indexed-view column metadata.");
            }
        }

        foreach (SqlServerViewMetadata view in views.Values)
        {
            bool hasIndexes = indexes.Keys.Any(key => key.ObjectId == view.ObjectId);
            if (hasIndexes != view.IsIndexed)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned inconsistent indexed-view summary metadata.");
            }
        }
    }

    private static void ValidateFullTextSnapshot(
        SqlServerCatalogSnapshot snapshot,
        IReadOnlySet<int> relationIds,
        IReadOnlySet<(int ObjectId, int ColumnId)> columnIds,
        IReadOnlyDictionary<int, SqlServerDataSpaceMetadata> dataSpaces,
        MetadataBudget budget,
        CancellationToken cancellationToken)
    {
        var catalogs = new Dictionary<int, SqlServerFullTextCatalogMetadata>();
        var catalogNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (SqlServerFullTextCatalogMetadata catalog in snapshot.FullTextCatalogs)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (catalog.FullTextCatalogId <= 0 ||
                !catalogs.TryAdd(catalog.FullTextCatalogId, catalog) ||
                !catalogNames.Add(catalog.Name))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate or invalid full-text catalog metadata.");
            }
            ValidateOptionalDataSpaceReference(
                catalog.DataSpaceId,
                dataSpaces,
                "full-text catalog");
            budget.Add(catalog.Name, isName: true);
        }

        var stoplists = new HashSet<int>();
        var stoplistNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (SqlServerFullTextStoplistMetadata stoplist in
                 snapshot.FullTextStoplists)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (stoplist.StoplistId <= 0 ||
                !stoplists.Add(stoplist.StoplistId) ||
                !stoplistNames.Add(stoplist.Name))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate or invalid full-text stoplist metadata.");
            }
            budget.Add(stoplist.Name, isName: true);
        }

        var propertyLists = new HashSet<int>();
        var propertyListNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (SqlServerSearchPropertyListMetadata propertyList in
                 snapshot.SearchPropertyLists)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (propertyList.PropertyListId <= 0 ||
                !propertyLists.Add(propertyList.PropertyListId) ||
                !propertyListNames.Add(propertyList.Name))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate or invalid search-property-list metadata.");
            }
            budget.Add(propertyList.Name, isName: true);
        }

        Dictionary<(int ObjectId, int IndexId), SqlServerIndexMetadata> indexes =
            snapshot.Indexes.ToDictionary(
                static item => (item.ObjectId, item.IndexId));
        var fullTextIndexes = new Dictionary<int, SqlServerFullTextIndexMetadata>();
        bool allowUnresolvedConfiguration = snapshot.Database.IsSysAdmin != true;
        foreach (SqlServerFullTextIndexMetadata index in snapshot.FullTextIndexes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!relationIds.Contains(index.ObjectId) ||
                !fullTextIndexes.TryAdd(index.ObjectId, index) ||
                !indexes.TryGetValue(
                    (index.ObjectId, index.UniqueIndexId),
                    out SqlServerIndexMetadata? uniqueIndex) ||
                !uniqueIndex.IsUnique ||
                index.FullTextCatalogId <= 0 ||
                !allowUnresolvedConfiguration &&
                !catalogs.ContainsKey(index.FullTextCatalogId) ||
                index.StoplistId is < 0 ||
                !allowUnresolvedConfiguration &&
                index.StoplistId is > 0 &&
                !stoplists.Contains(index.StoplistId.Value) ||
                index.PropertyListId is <= 0 ||
                !allowUnresolvedConfiguration &&
                index.PropertyListId is > 0 &&
                !propertyLists.Contains(index.PropertyListId.Value))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned invalid or unresolved full-text index metadata.");
            }
            ValidateOptionalDataSpaceReference(
                index.DataSpaceId,
                dataSpaces,
                "full-text index");
            if ((snapshot.Instance.ProductMajorVersion >= 17) !=
                (index.IndexVersion is not null) ||
                index.IndexVersion < 0)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned inconsistent full-text index-version metadata.");
            }
            budget.Add(index.ChangeTrackingState);
            budget.Add(index.ChangeTrackingStateDescription);
        }

        var memberships = new HashSet<(int ObjectId, int ColumnId)>();
        var membershipCounts = new Dictionary<int, int>();
        foreach (SqlServerFullTextIndexColumnMetadata column in
                 snapshot.FullTextIndexColumns)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!fullTextIndexes.ContainsKey(column.ObjectId) ||
                column.ColumnId <= 0 ||
                !columnIds.Contains((column.ObjectId, column.ColumnId)) ||
                column.TypeColumnId is int typeColumnId &&
                (typeColumnId <= 0 ||
                 typeColumnId == column.ColumnId ||
                 !columnIds.Contains((column.ObjectId, typeColumnId))) ||
                column.LanguageId < 0 ||
                !memberships.Add((column.ObjectId, column.ColumnId)))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned full-text index-column metadata.");
            }
            membershipCounts[column.ObjectId] =
                membershipCounts.GetValueOrDefault(column.ObjectId) + 1;
        }
        if (fullTextIndexes.Keys.Any(objectId =>
                membershipCounts.GetValueOrDefault(objectId) == 0))
        {
            throw new SqlServerMigrationException(
                "SQL Server returned a full-text index without indexed columns.");
        }
    }

    private static void ValidatePartitionSnapshot(
        SqlServerCatalogSnapshot snapshot,
        IReadOnlySet<int> relationIds,
        IReadOnlySet<int> tableIds,
        IReadOnlyDictionary<int, SqlServerDataSpaceMetadata> dataSpaces,
        MetadataBudget budget,
        SqlServerInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        var functions = new Dictionary<int, SqlServerPartitionFunctionMetadata>();
        var functionNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (SqlServerPartitionFunctionMetadata function in
                 snapshot.PartitionFunctions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (function.FunctionId <= 0 ||
                function.Fanout <= 0 ||
                !functions.TryAdd(function.FunctionId, function) ||
                !functionNames.Add(function.Name))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate or invalid partition-function metadata.");
            }
            budget.Add(function.Name, isName: true);
        }

        var parameters = new Dictionary<
            (int FunctionId, int ParameterId),
            SqlServerPartitionParameterMetadata>();
        var parameterIdsByFunction = new Dictionary<int, SortedSet<int>>();
        foreach (SqlServerPartitionParameterMetadata parameter in
                 snapshot.PartitionParameters)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!functions.ContainsKey(parameter.FunctionId) ||
                parameter.ParameterId <= 0 ||
                !parameters.TryAdd(
                    (parameter.FunctionId, parameter.ParameterId),
                    parameter))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned partition-parameter metadata.");
            }
            budget.Add(parameter.TypeSchema, isName: true);
            budget.Add(parameter.TypeName, isName: true);
            budget.Add(parameter.SystemTypeName, isName: true);
            budget.Add(parameter.Collation);
            ValidatePartitionParameterShape(parameter);
            if (!parameterIdsByFunction.TryGetValue(
                    parameter.FunctionId,
                    out SortedSet<int>? ownedParameterIds))
            {
                ownedParameterIds = [];
                parameterIdsByFunction.Add(
                    parameter.FunctionId,
                    ownedParameterIds);
            }
            ownedParameterIds.Add(parameter.ParameterId);
        }
        foreach (SqlServerPartitionFunctionMetadata function in functions.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!parameterIdsByFunction.TryGetValue(
                    function.FunctionId,
                    out SortedSet<int>? ownedParameterIds) ||
                ownedParameterIds.Count != 1 ||
                !ownedParameterIds.Contains(1))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned an unsupported partition-function parameter shape.");
            }
        }

        var boundaries = new HashSet<(int FunctionId, int BoundaryId, int ParameterId)>();
        var boundaryIdsByFunction = new Dictionary<int, SortedSet<int>>();
        foreach (SqlServerPartitionRangeValueMetadata boundary in
                 snapshot.PartitionRangeValues)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (boundary.BoundaryId <= 0 ||
                !parameters.ContainsKey((
                    boundary.FunctionId,
                    boundary.ParameterId)) ||
                !boundaries.Add((
                    boundary.FunctionId,
                    boundary.BoundaryId,
                    boundary.ParameterId)))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned partition boundary metadata.");
            }
            ValidatePartitionBoundary(boundary, limits);
            budget.Add(boundary.BaseType);
            budget.Add(boundary.Collation);
            budget.Add(boundary.ValueHex);
            if (!boundaryIdsByFunction.TryGetValue(
                    boundary.FunctionId,
                    out SortedSet<int>? ownedBoundaryIds))
            {
                ownedBoundaryIds = [];
                boundaryIdsByFunction.Add(
                    boundary.FunctionId,
                    ownedBoundaryIds);
            }
            ownedBoundaryIds.Add(boundary.BoundaryId);
        }
        foreach (SqlServerPartitionFunctionMetadata function in functions.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int expectedBoundaryCount = checked(function.Fanout - 1);
            boundaryIdsByFunction.TryGetValue(
                function.FunctionId,
                out SortedSet<int>? ownedBoundaryIds);
            if ((ownedBoundaryIds?.Count ?? 0) != expectedBoundaryCount)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned partition boundaries inconsistent with function fanout.");
            }
            int expectedBoundaryId = 1;
            foreach (int boundaryId in ownedBoundaryIds ?? [])
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (boundaryId != expectedBoundaryId++)
                {
                    throw new SqlServerMigrationException(
                        "SQL Server returned partition boundaries inconsistent with function fanout.");
                }
            }
        }

        var schemes = new Dictionary<int, SqlServerPartitionSchemeMetadata>();
        foreach (SqlServerPartitionSchemeMetadata scheme in
                 snapshot.PartitionSchemes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!dataSpaces.ContainsKey(scheme.DataSpaceId) ||
                !functions.ContainsKey(scheme.FunctionId) ||
                !schemes.TryAdd(scheme.DataSpaceId, scheme))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unresolved partition-scheme metadata.");
            }
        }

        var destinations =
            new Dictionary<(int SchemeId, int DestinationId), int>();
        var destinationIdsByScheme = new Dictionary<int, SortedSet<int>>();
        foreach (SqlServerPartitionSchemeDestinationMetadata destination in
                 snapshot.PartitionSchemeDestinations)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!schemes.ContainsKey(destination.PartitionSchemeId) ||
                destination.DestinationId <= 0 ||
                !dataSpaces.ContainsKey(destination.DataSpaceId) ||
                !destinations.TryAdd(
                    (
                        destination.PartitionSchemeId,
                        destination.DestinationId),
                    destination.DataSpaceId))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unresolved partition-scheme destination metadata.");
            }
            if (!destinationIdsByScheme.TryGetValue(
                    destination.PartitionSchemeId,
                    out SortedSet<int>? ownedDestinationIds))
            {
                ownedDestinationIds = [];
                destinationIdsByScheme.Add(
                    destination.PartitionSchemeId,
                    ownedDestinationIds);
            }
            ownedDestinationIds.Add(destination.DestinationId);
        }
        foreach (SqlServerPartitionSchemeMetadata scheme in schemes.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int fanout = functions[scheme.FunctionId].Fanout;
            if (!destinationIdsByScheme.TryGetValue(
                    scheme.DataSpaceId,
                    out SortedSet<int>? ownedDestinationIds) ||
                ownedDestinationIds.Count < fanout)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned partition-scheme destinations inconsistent with function fanout.");
            }
            int expectedDestinationId = 1;
            foreach (int destinationId in ownedDestinationIds)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (destinationId != expectedDestinationId++)
                {
                    throw new SqlServerMigrationException(
                        "SQL Server returned partition-scheme destinations inconsistent with function fanout.");
                }
            }
        }

        Dictionary<(int ObjectId, int IndexId), SqlServerIndexMetadata> indexes =
            snapshot.Indexes.ToDictionary(
                static item => (item.ObjectId, item.IndexId));
        var partitionKeys = new HashSet<(int ObjectId, int IndexId, int Number)>();
        var partitionsByOwner = new Dictionary<
            (int ObjectId, int IndexId),
            SortedDictionary<int, SqlServerIndexPartitionMetadata>>();
        foreach (SqlServerIndexPartitionMetadata partition in
                 snapshot.IndexPartitions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!relationIds.Contains(partition.ObjectId) ||
                partition.IndexId < 0 ||
                partition.IndexId == 0 && !tableIds.Contains(partition.ObjectId) ||
                partition.IndexId > 0 &&
                (!indexes.TryGetValue(
                        (partition.ObjectId, partition.IndexId),
                        out SqlServerIndexMetadata? indexDefinition) ||
                 indexDefinition.DataSpaceId !=
                    partition.DefinitionDataSpaceId.GetValueOrDefault()) ||
                partition.PartitionNumber <= 0 ||
                !partitionKeys.Add((
                    partition.ObjectId,
                    partition.IndexId,
                    partition.PartitionNumber)) ||
                partition.DefinitionDataSpaceId is int definitionDataSpaceId &&
                !dataSpaces.ContainsKey(definitionDataSpaceId) ||
                partition.StorageDataSpaceId is int storageDataSpaceId &&
                !dataSpaces.ContainsKey(storageDataSpaceId) ||
                (partition.XmlCompression is null) !=
                (partition.XmlCompressionDescription is null))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned physical-partition metadata.");
            }
            budget.Add(partition.DataCompressionDescription);
            budget.Add(partition.XmlCompressionDescription);
            if (!partitionsByOwner.TryGetValue(
                    (partition.ObjectId, partition.IndexId),
                    out SortedDictionary<
                        int,
                        SqlServerIndexPartitionMetadata>? ownedPartitions))
            {
                ownedPartitions = [];
                partitionsByOwner.Add(
                    (partition.ObjectId, partition.IndexId),
                    ownedPartitions);
            }
            ownedPartitions.Add(partition.PartitionNumber, partition);
        }
        foreach (SortedDictionary<int, SqlServerIndexPartitionMetadata>
                 ownedPartitions in partitionsByOwner.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int expectedPartitionNumber = 1;
            int? definitionDataSpaceId = null;
            bool isFirstPartition = true;
            foreach (SqlServerIndexPartitionMetadata partition in
                     ownedPartitions.Values)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (partition.PartitionNumber != expectedPartitionNumber++)
                {
                    throw new SqlServerMigrationException(
                        "SQL Server returned noncontiguous physical partition numbers.");
                }
                if (isFirstPartition)
                {
                    definitionDataSpaceId = partition.DefinitionDataSpaceId;
                    isFirstPartition = false;
                }
                else if (partition.DefinitionDataSpaceId != definitionDataSpaceId)
                {
                    throw new SqlServerMigrationException(
                        "SQL Server returned inconsistent physical-partition data-space metadata.");
                }
            }

            if (definitionDataSpaceId is int partitionSchemeDataSpaceId &&
                schemes.TryGetValue(
                    partitionSchemeDataSpaceId,
                    out SqlServerPartitionSchemeMetadata? partitionScheme))
            {
                int fanout = functions[partitionScheme.FunctionId].Fanout;
                if (ownedPartitions.Count != fanout)
                {
                    throw new SqlServerMigrationException(
                        "SQL Server returned inconsistent physical-partition data-space metadata.");
                }
                foreach (SqlServerIndexPartitionMetadata partition in
                         ownedPartitions.Values)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (!destinations.TryGetValue(
                            (
                                partitionSchemeDataSpaceId,
                                partition.PartitionNumber),
                            out int expectedStorageDataSpaceId) ||
                        partition.StorageDataSpaceId !=
                            expectedStorageDataSpaceId)
                    {
                        throw new SqlServerMigrationException(
                            "SQL Server returned inconsistent physical-partition data-space metadata.");
                    }
                }
            }
            else
            {
                SqlServerIndexPartitionMetadata onlyPartition =
                    ownedPartitions.Values.First();
                if (ownedPartitions.Count != 1 ||
                    onlyPartition.StorageDataSpaceId != definitionDataSpaceId)
                {
                    throw new SqlServerMigrationException(
                        "SQL Server returned inconsistent physical-partition data-space metadata.");
                }
            }
        }
    }

    private static IEnumerable<string?> PhysicalSnapshotFields(
        SqlServerCatalogSnapshot snapshot)
    {
        foreach (SqlServerFullTextCatalogMetadata catalog in
                 snapshot.FullTextCatalogs
                     .OrderBy(static item => item.FullTextCatalogId))
        {
            yield return "full-text-catalog";
            yield return Invariant(catalog.FullTextCatalogId);
            yield return catalog.Name;
            yield return Boolean(catalog.IsDefault);
            yield return Boolean(catalog.IsAccentSensitivityOn);
            yield return Invariant(catalog.DataSpaceId);
        }

        foreach (SqlServerFullTextStoplistMetadata stoplist in
                 snapshot.FullTextStoplists
                     .OrderBy(static item => item.StoplistId))
        {
            yield return "full-text-stoplist";
            yield return Invariant(stoplist.StoplistId);
            yield return stoplist.Name;
        }

        foreach (SqlServerSearchPropertyListMetadata propertyList in
                 snapshot.SearchPropertyLists
                     .OrderBy(static item => item.PropertyListId))
        {
            yield return "search-property-list";
            yield return Invariant(propertyList.PropertyListId);
            yield return propertyList.Name;
        }

        foreach (SqlServerFullTextIndexMetadata index in snapshot.FullTextIndexes
                     .OrderBy(static item => item.ObjectId))
        {
            yield return "full-text-index";
            yield return Invariant(index.ObjectId);
            yield return Invariant(index.UniqueIndexId);
            yield return OptionalInvariant(index.IndexVersion);
            yield return Invariant(index.FullTextCatalogId);
            yield return Boolean(index.IsEnabled);
            yield return index.ChangeTrackingState;
            yield return index.ChangeTrackingStateDescription;
            yield return OptionalInvariant(index.StoplistId);
            yield return Invariant(index.DataSpaceId);
            yield return OptionalInvariant(index.PropertyListId);
        }

        foreach (SqlServerFullTextIndexColumnMetadata column in
                 snapshot.FullTextIndexColumns
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.ColumnId))
        {
            yield return "full-text-index-column";
            yield return Invariant(column.ObjectId);
            yield return Invariant(column.ColumnId);
            yield return OptionalInvariant(column.TypeColumnId);
            yield return Invariant(column.LanguageId);
            yield return Boolean(column.StatisticalSemantics);
        }

        foreach (SqlServerDataSpaceMetadata dataSpace in snapshot.DataSpaces
                     .OrderBy(static item => item.DataSpaceId))
        {
            yield return "data-space";
            yield return Invariant(dataSpace.DataSpaceId);
            yield return dataSpace.Name;
            yield return dataSpace.Type;
            yield return dataSpace.TypeDescription;
            yield return Boolean(dataSpace.IsDefault);
            yield return Boolean(dataSpace.IsSystem);
            yield return NullableBoolean(dataSpace.IsReadOnly);
        }

        foreach (SqlServerPartitionFunctionMetadata function in
                 snapshot.PartitionFunctions
                     .OrderBy(static item => item.FunctionId))
        {
            yield return "partition-function";
            yield return Invariant(function.FunctionId);
            yield return function.Name;
            yield return Invariant(function.Fanout);
            yield return Boolean(function.BoundaryValueOnRight);
            yield return Boolean(function.IsSystem);
        }

        foreach (SqlServerPartitionParameterMetadata parameter in
                 snapshot.PartitionParameters
                     .OrderBy(static item => item.FunctionId)
                     .ThenBy(static item => item.ParameterId))
        {
            yield return "partition-parameter";
            yield return Invariant(parameter.FunctionId);
            yield return Invariant(parameter.ParameterId);
            yield return parameter.TypeSchema;
            yield return parameter.TypeName;
            yield return parameter.SystemTypeName;
            yield return Invariant(parameter.MaxLength);
            yield return Invariant(parameter.Precision);
            yield return Invariant(parameter.Scale);
            yield return parameter.Collation;
        }

        foreach (SqlServerPartitionRangeValueMetadata boundary in
                 snapshot.PartitionRangeValues
                     .OrderBy(static item => item.FunctionId)
                     .ThenBy(static item => item.BoundaryId)
                     .ThenBy(static item => item.ParameterId))
        {
            yield return "partition-boundary";
            yield return Invariant(boundary.FunctionId);
            yield return Invariant(boundary.BoundaryId);
            yield return Invariant(boundary.ParameterId);
            yield return Boolean(boundary.IsNull);
            yield return boundary.BaseType;
            yield return OptionalInvariant(boundary.MaxLength);
            yield return OptionalInvariant(boundary.Precision);
            yield return OptionalInvariant(boundary.Scale);
            yield return boundary.Collation;
            yield return OptionalInvariant(boundary.ValueBytes);
            yield return boundary.ValueHex;
        }

        foreach (SqlServerPartitionSchemeMetadata scheme in
                 snapshot.PartitionSchemes
                     .OrderBy(static item => item.DataSpaceId))
        {
            yield return "partition-scheme";
            yield return Invariant(scheme.DataSpaceId);
            yield return Invariant(scheme.FunctionId);
        }

        foreach (SqlServerPartitionSchemeDestinationMetadata destination in
                 snapshot.PartitionSchemeDestinations
                     .OrderBy(static item => item.PartitionSchemeId)
                     .ThenBy(static item => item.DestinationId))
        {
            yield return "partition-scheme-destination";
            yield return Invariant(destination.PartitionSchemeId);
            yield return Invariant(destination.DestinationId);
            yield return Invariant(destination.DataSpaceId);
        }

        foreach (SqlServerIndexPartitionMetadata partition in
                 snapshot.IndexPartitions
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.IndexId)
                     .ThenBy(static item => item.PartitionNumber))
        {
            yield return "index-partition";
            yield return Invariant(partition.ObjectId);
            yield return Invariant(partition.IndexId);
            yield return Invariant(partition.PartitionNumber);
            yield return Invariant(partition.DataCompression);
            yield return partition.DataCompressionDescription;
            yield return NullableBoolean(partition.XmlCompression);
            yield return partition.XmlCompressionDescription;
            yield return OptionalInvariant(partition.DefinitionDataSpaceId);
            yield return OptionalInvariant(partition.StorageDataSpaceId);
        }
    }

    private static void ValidateOptionalDataSpaceReference(
        int dataSpaceId,
        IReadOnlyDictionary<int, SqlServerDataSpaceMetadata> dataSpaces,
        string description)
    {
        if (dataSpaceId < 0 ||
            dataSpaceId > 0 && !dataSpaces.ContainsKey(dataSpaceId))
        {
            throw new SqlServerMigrationException(
                $"SQL Server returned an unresolved {description} data-space reference.");
        }
    }

    private static void ValidatePartitionParameterShape(
        SqlServerPartitionParameterMetadata parameter)
    {
        byte maximumPrecision = string.Equals(
                parameter.SystemTypeName,
                "float",
                StringComparison.OrdinalIgnoreCase)
            ? (byte)53
            : (byte)38;
        if (parameter.MaxLength < -1 ||
            parameter.Precision > maximumPrecision ||
            parameter.Scale > parameter.Precision)
        {
            throw new SqlServerMigrationException(
                "SQL Server returned invalid partition-parameter type metadata.");
        }
    }

    private static void ValidatePartitionBoundary(
        SqlServerPartitionRangeValueMetadata boundary,
        SqlServerInspectionLimits limits)
    {
        bool hasTypedValue =
            boundary.BaseType is not null ||
            boundary.MaxLength is not null ||
            boundary.Precision is not null ||
            boundary.Scale is not null ||
            boundary.Collation is not null ||
            boundary.ValueBytes is not null ||
            boundary.ValueHex is not null;
        if (boundary.IsNull)
        {
            if (hasTypedValue)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned typed payload metadata for a null partition boundary.");
            }
            return;
        }

        if (boundary.BaseType is null ||
            boundary.MaxLength is null ||
            boundary.Precision is null ||
            boundary.Scale is null ||
            boundary.ValueBytes is not int valueBytes ||
            valueBytes < 0 ||
            valueBytes > limits.MaxPartitionBoundaryBytes ||
            boundary.ValueHex is null ||
            boundary.ValueHex.Length != checked(valueBytes * 2) ||
            boundary.ValueHex.AsSpan().IndexOfAnyExcept(
                "0123456789abcdefABCDEF".AsSpan()) >= 0)
        {
            throw new SqlServerMigrationException(
                "SQL Server returned invalid typed partition-boundary metadata.");
        }
    }

    private static string PartitionBoundaryDigest(
        SqlServerPartitionRangeValueMetadata boundary) =>
        "sha256:" + SqlServerStableDigest.Text(
            "csharpdb-sqlserver-partition-boundary/v1",
            Boolean(boundary.IsNull),
            boundary.BaseType,
            OptionalInvariant(boundary.MaxLength),
            OptionalInvariant(boundary.Precision),
            OptionalInvariant(boundary.Scale),
            boundary.Collation,
            OptionalInvariant(boundary.ValueBytes),
            boundary.ValueHex?.ToLowerInvariant());

    private static string PartitionParameterObjectId(
        SqlServerPartitionFunctionMetadata function,
        int parameterId) =>
        ObjectId(
            "partition-parameter",
            function.Name,
            Invariant(parameterId));

    private static string FormatPartitionParameterNativeType(
        SqlServerPartitionParameterMetadata parameter)
    {
        string type = $"{parameter.TypeSchema}.{parameter.TypeName}";
        if (!string.Equals(
                parameter.TypeSchema,
                "sys",
                StringComparison.Ordinal) ||
            !string.Equals(
                parameter.TypeName,
                parameter.SystemTypeName,
                StringComparison.Ordinal))
        {
            return type;
        }

        string systemType = parameter.SystemTypeName.ToLowerInvariant();
        if (systemType is "decimal" or "numeric")
        {
            return $"{type}({Invariant(parameter.Precision)}," +
                $"{Invariant(parameter.Scale)})";
        }
        if (systemType is "time" or "datetime2" or "datetimeoffset")
            return $"{type}({Invariant(parameter.Scale)})";
        if (IsLengthType(systemType))
        {
            string length = parameter.MaxLength < 0
                ? "max"
                : Invariant(systemType is "nchar" or "nvarchar"
                    ? parameter.MaxLength / 2
                    : parameter.MaxLength);
            return $"{type}({length})";
        }
        return type;
    }

    private static MigrationDiagnostic PartitionDiagnostic(
        string objectId,
        string summary) =>
        Diagnostic(
            objectId,
            PartitioningRule,
            MigrationDiagnosticSeverity.Error,
            MigrationCompatibilityStatus.Unsupported,
            summary,
            "SQL Server partition routing and boundary semantics are retained as provider metadata without a CSharpDB partition-function or partition-scheme lowering contract.",
            "Choose and validate an explicit target sharding or storage design.",
            canOverride: false);

    private static string[] OptionalReference(
        IReadOnlyDictionary<int, string> objectIds,
        int nativeId) =>
        nativeId > 0 && objectIds.TryGetValue(nativeId, out string? objectId)
            ? [objectId]
            : [];

    private static string? OptionalInvariant<T>(T? value)
        where T : struct, IFormattable =>
        value is T present ? Invariant(present) : null;

    private sealed record PhysicalRelation(
        int ObjectId,
        string SchemaName,
        string Name,
        string Id,
        bool IsView);

    private sealed record PhysicalColumn(
        int ObjectId,
        int ColumnId,
        string Name,
        string Id);
}
