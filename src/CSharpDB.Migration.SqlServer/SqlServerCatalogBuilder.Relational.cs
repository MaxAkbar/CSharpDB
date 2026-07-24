using System.Globalization;
using System.Numerics;
using CSharpDB.Migration;

namespace CSharpDB.Migration.SqlServer;

internal static partial class SqlServerCatalogBuilder
{
    private static int RelationalObjectCapacity(SqlServerCatalogSnapshot snapshot) =>
        checked(
            snapshot.Keys.Count +
            snapshot.Indexes.Count +
            snapshot.ForeignKeys.Count +
            snapshot.Checks.Count +
            snapshot.Sequences.Count);

    private static void AddRelationalObjects(
        SqlServerCatalogSnapshot snapshot,
        SqlServerScriptDomAnalysisSnapshot scriptDomAnalysis,
        IReadOnlyDictionary<int, (SqlServerSchemaMetadata Metadata, string ObjectId)> schemasById,
        IReadOnlyDictionary<int, (SqlServerTableMetadata Metadata, string Id)> tablesByObjectId,
        IReadOnlyDictionary<
            (int ObjectId, int ColumnId),
            (SqlServerColumnMetadata Metadata, string Id)> columnsByCatalogId,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics,
        CancellationToken cancellationToken)
    {
        Dictionary<(int ObjectId, int IndexId), SqlServerIndexMetadata> indexesById =
            snapshot.Indexes.ToDictionary(
                static item => (item.ObjectId, item.IndexId));
        Dictionary<(int ObjectId, int IndexId), SqlServerIndexColumnMetadata[]> indexColumnsById =
            snapshot.IndexColumns
                .GroupBy(static item => (item.ObjectId, item.IndexId))
                .ToDictionary(
                    static group => group.Key,
                    static group => group
                        .OrderBy(static item => item.IndexColumnId)
                        .ToArray());
        Dictionary<int, SqlServerForeignKeyColumnMetadata[]> foreignKeyColumnsById =
            snapshot.ForeignKeyColumns
                .GroupBy(static item => item.ConstraintObjectId)
                .ToDictionary(
                    static group => group.Key,
                    static group => group
                        .OrderBy(static item => item.ConstraintColumnId)
                        .ToArray());

        var keysByIndex = new Dictionary<(int ObjectId, int IndexId), BuiltKey>();
        foreach (SqlServerKeyMetadata key in snapshot.Keys
                     .OrderBy(static item => item.ParentObjectId)
                     .ThenBy(static item => item.ObjectId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            (SqlServerTableMetadata table, string tableId) =
                tablesByObjectId[key.ParentObjectId];
            SqlServerSchemaMetadata schema = schemasById[table.SchemaId].Metadata;
            indexesById.TryGetValue(
                (key.ParentObjectId, key.UniqueIndexId),
                out SqlServerIndexMetadata? backingIndex);
            indexColumnsById.TryGetValue(
                (key.ParentObjectId, key.UniqueIndexId),
                out SqlServerIndexColumnMetadata[]? backingColumns);
            backingColumns ??= [];

            SqlServerIndexColumnMetadata[] keyColumns = backingColumns
                .Where(static item => item.KeyOrdinal > 0 && !item.IsIncluded)
                .OrderBy(static item => item.KeyOrdinal)
                .ThenBy(static item => item.IndexColumnId)
                .ToArray();
            bool contiguous = HasContiguousKeyOrdinals(keyColumns);
            var resolvedColumns =
                new List<(SqlServerIndexColumnMetadata IndexColumn, SqlServerColumnMetadata Column, string Id)>();
            foreach (SqlServerIndexColumnMetadata indexColumn in keyColumns)
            {
                if (columnsByCatalogId.TryGetValue(
                        (indexColumn.ObjectId, indexColumn.ColumnId),
                        out (SqlServerColumnMetadata Metadata, string Id) column))
                {
                    resolvedColumns.Add((indexColumn, column.Metadata, column.Id));
                }
            }

            bool completeMembership =
                backingIndex is not null &&
                keyColumns.Length > 0 &&
                contiguous &&
                keyColumns
                    .Select(static item => item.ColumnId)
                    .Distinct()
                    .Count() == keyColumns.Length &&
                resolvedColumns.Count == keyColumns.Length;
            bool nullableUnique =
                string.Equals(key.Type, "UQ", StringComparison.Ordinal) &&
                resolvedColumns.Any(static item => item.Column.IsNullable);
            bool invalidPrimaryNullability =
                string.Equals(key.Type, "PK", StringComparison.Ordinal) &&
                resolvedColumns.Any(static item => item.Column.IsNullable);
            bool inconsistentBacking =
                backingIndex is not null &&
                (!backingIndex.IsUnique ||
                 string.Equals(key.Type, "PK", StringComparison.Ordinal) !=
                 backingIndex.IsPrimaryKey ||
                 string.Equals(key.Type, "UQ", StringComparison.Ordinal) !=
                 backingIndex.IsUniqueConstraint);
            bool unsupportedBacking =
                backingIndex is not null &&
                (inconsistentBacking ||
                 HasUnsupportedConstraintBackingShape(backingIndex, backingColumns));
            string logicalKind = key.Type switch
            {
                "PK" => "primary",
                "UQ" => "unique",
                _ => "unknown",
            };
            string effectiveKind = !completeMembership
                ? "sqlserver-unresolved-key"
                : invalidPrimaryNullability
                    ? "sqlserver-invalid-primary-key"
                : nullableUnique
                    ? "sqlserver-null-sensitive-unique"
                    : unsupportedBacking
                        ? "sqlserver-unsupported-key-index"
                        : logicalKind;
            string keyId = ObjectId("key", schema.Name, table.Name, key.Name);
            SqlServerScriptDomDefinitionAnalysis? filterAnalysis =
                backingIndex is null
                    ? null
                    : GetScriptDomAnalysis(
                        scriptDomAnalysis,
                        SqlServerScriptDomDefinitionKind.IndexFilterPredicate,
                        backingIndex.ObjectId,
                        backingIndex.IndexId,
                        backingIndex.FilterDefinition is not null);

            var facets = new List<MigrationCatalogFacet>
            {
                Facet("kind", effectiveKind),
                Facet("sqlServerObjectId", Invariant(key.ObjectId)),
                Facet("sqlServerConstraintType", key.Type),
                Facet("sqlServerUniqueIndexId", Invariant(key.UniqueIndexId)),
                Facet("sqlServerSystemNamed", Boolean(key.IsSystemNamed)),
                Facet("sqlServerMembershipComplete", Boolean(completeMembership)),
            };
            if (backingIndex is not null)
            {
                AddPhysicalIndexFacets(facets, backingIndex, backingColumns);
                AddDefinitionDigestFacets(
                    facets,
                    "sqlServerFilterDefinition",
                    "csharpdb-sqlserver-filter-definition/v1",
                    backingIndex.FilterDefinitionBytes,
                    backingIndex.FilterDefinition);
                if (backingIndex.HasFilter)
                {
                    AddScriptDomFacets(
                        facets,
                        "sqlServerFilterTsqlAnalysis",
                        "sqlServerFilterTsql",
                        filterAnalysis);
                }
            }

            MigrationObjectReference[] members = resolvedColumns
                .Select((item, ordinal) => Member(
                    item.Id,
                    MigrationObjectReferenceRoles.Column,
                    ordinal))
                .ToArray();
            string[] dependencies = resolvedColumns
                .Select(static item => item.Id)
                .Distinct(StringComparer.Ordinal)
                .ToArray();
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = keyId,
                Kind = MigrationObjectKind.Key,
                ParentObjectId = tableId,
                SourceNamespace = schema.Name,
                SourceName = key.Name,
                Facets = facets.AsReadOnly(),
                Members = members,
                DependsOn = dependencies,
            });

            keysByIndex.Add(
                (key.ParentObjectId, key.UniqueIndexId),
                new BuiltKey(
                    keyId,
                    effectiveKind,
                    completeMembership,
                    resolvedColumns
                        .Select(static item => item.IndexColumn)
                        .ToArray()));

            if (!completeMembership)
            {
                diagnostics.Add(Diagnostic(
                    keyId,
                    "MIG-SQLSERVER-KEY-MEMBERSHIP-UNKNOWN-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unknown,
                    "The SQL Server key membership could not be proven.",
                    "The backing index, its key columns, or a contiguous catalog ordering was not fully visible.",
                    "Restore complete metadata visibility and inspect the key again.",
                    canOverride: false));
            }
            if (nullableUnique)
            {
                diagnostics.Add(Diagnostic(
                    keyId,
                    "MIG-SQLSERVER-NULLABLE-UNIQUE-SEMANTICS-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unsupported,
                    "The nullable unique-key semantics are not target-equivalent.",
                    "SQL Server and CSharpDB do not apply the same uniqueness rule to every NULL-containing key tuple.",
                    "Choose and validate an explicit target constraint or filtered-index design.",
                    canOverride: false));
            }
            if (invalidPrimaryNullability || unsupportedBacking)
            {
                diagnostics.Add(Diagnostic(
                    keyId,
                    "MIG-SQLSERVER-KEY-BACKING-INDEX-UNSUPPORTED-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unsupported,
                    "The key has an unsupported SQL Server backing-index shape.",
                    "Nullable primary-key metadata and disabled, hypothetical, inconsistent, filtered, included-column, partitioned, special-kind, or duplicate-ignoring backing indexes cannot be silently lowered.",
                    "Rebuild the key on an ordinary trusted rowstore index or provide a reviewed target design.",
                    canOverride: false));
            }
            AddFilterAnalysisDiagnostic(
                keyId,
                backingIndex,
                filterAnalysis,
                diagnostics);
        }

        var indexObjectIds = new Dictionary<(int ObjectId, int IndexId), string>();
        foreach (SqlServerIndexMetadata index in snapshot.Indexes
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.IndexId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (keysByIndex.ContainsKey((index.ObjectId, index.IndexId)))
                continue;

            (SqlServerTableMetadata table, string tableId) =
                tablesByObjectId[index.ObjectId];
            SqlServerSchemaMetadata schema = schemasById[table.SchemaId].Metadata;
            indexColumnsById.TryGetValue(
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
                keyColumns.All(item => columnsByCatalogId.ContainsKey(
                    (item.ObjectId, item.ColumnId)));
            bool hasUnresolvedPhysicalColumn = allColumns.Any(item =>
                !IsHeapRid(index, item) &&
                (item.ColumnId <= 0 ||
                 !columnsByCatalogId.ContainsKey((item.ObjectId, item.ColumnId)))) ||
                allColumns.Any(item => IsUnclassifiedRowstoreColumn(index, item));
            bool hasIncludedColumns = allColumns.Any(static item => item.IsIncluded);
            bool hasDescendingKeys = keyColumns.Any(static item => item.IsDescending);
            bool isPartitioned = allColumns.Any(static item => item.PartitionOrdinal > 0);
            bool nullableUnique = index.IsUnique &&
                keyColumns.Any(item =>
                    columnsByCatalogId.TryGetValue(
                        (item.ObjectId, item.ColumnId),
                        out (SqlServerColumnMetadata Metadata, string Id) column) &&
                    column.Metadata.IsNullable);
            bool orphanedConstraintBacking =
                index.IsPrimaryKey || index.IsUniqueConstraint;
            string indexKind = IndexKind(index);
            if (!completeMembership || hasUnresolvedPhysicalColumn)
                indexKind = "sqlserver-unresolved-index";
            else if (orphanedConstraintBacking)
                indexKind = "sqlserver-unresolved-constraint-index";
            else if (nullableUnique)
                indexKind = "sqlserver-null-sensitive-unique";
            else if (index.IsDisabled)
                indexKind = "sqlserver-disabled-index";
            else if (index.IsHypothetical)
                indexKind = "sqlserver-hypothetical-index";
            else if (index.IgnoreDuplicateKey)
                indexKind = "sqlserver-ignore-duplicate-key";
            else if (isPartitioned)
                indexKind = "sqlserver-partitioned-index";

            string indexId = ObjectId("index", schema.Name, table.Name, index.Name);
            indexObjectIds.Add((index.ObjectId, index.IndexId), indexId);
            SqlServerScriptDomDefinitionAnalysis? filterAnalysis =
                GetScriptDomAnalysis(
                    scriptDomAnalysis,
                    SqlServerScriptDomDefinitionKind.IndexFilterPredicate,
                    index.ObjectId,
                    index.IndexId,
                    index.FilterDefinition is not null);
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("kind", indexKind),
                Facet("unique", Boolean(index.IsUnique)),
                Facet("sqlServerMembershipComplete", Boolean(completeMembership)),
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

            (SqlServerIndexColumnMetadata IndexColumn, string Id)[] resolvedKeyColumns =
                keyColumns
                    .Where(item => columnsByCatalogId.ContainsKey(
                        (item.ObjectId, item.ColumnId)))
                    .Select(item => (
                        item,
                        columnsByCatalogId[(item.ObjectId, item.ColumnId)].Id))
                    .ToArray();
            string[] dependencies = allColumns
                .OrderBy(static item => item.IndexColumnId)
                .Where(item => columnsByCatalogId.ContainsKey(
                    (item.ObjectId, item.ColumnId)))
                .Select(item => columnsByCatalogId[(item.ObjectId, item.ColumnId)].Id)
                .Distinct(StringComparer.Ordinal)
                .ToArray();
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = indexId,
                Kind = MigrationObjectKind.Index,
                ParentObjectId = tableId,
                SourceNamespace = schema.Name,
                SourceName = index.Name,
                Facets = facets.AsReadOnly(),
                Members = resolvedKeyColumns
                    .Select((item, ordinal) => Member(
                        item.Id,
                        MigrationObjectReferenceRoles.Column,
                        ordinal))
                    .ToArray(),
                DependsOn = dependencies,
            });

            bool unsupported =
                !string.Equals(indexKind, "standard", StringComparison.Ordinal) ||
                index.HasFilter ||
                hasIncludedColumns ||
                hasDescendingKeys;
            if (unsupported)
            {
                diagnostics.Add(Diagnostic(
                    indexId,
                    "MIG-SQLSERVER-INDEX-SHAPE-UNSUPPORTED-001",
                    MigrationDiagnosticSeverity.Error,
                    completeMembership
                        ? MigrationCompatibilityStatus.Unsupported
                        : MigrationCompatibilityStatus.Unknown,
                    "The SQL Server index shape is not directly target-compatible.",
                    "Only complete, enabled, nonfiltered, ascending, nonclustered rowstore indexes without included or partition columns are admitted by this checkpoint.",
                    "Simplify the index or define and test an explicit target index design.",
                    canOverride: false));
            }
            AddFilterAnalysisDiagnostic(
                indexId,
                index,
                filterAnalysis,
                diagnostics);
        }

        foreach (SqlServerForeignKeyMetadata foreignKey in snapshot.ForeignKeys
                     .OrderBy(static item => item.ParentObjectId)
                     .ThenBy(static item => item.ObjectId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            (SqlServerTableMetadata childTable, string childTableId) =
                tablesByObjectId[foreignKey.ParentObjectId];
            SqlServerSchemaMetadata childSchema = schemasById[childTable.SchemaId].Metadata;
            string foreignKeyId = ObjectId(
                "foreign-key",
                childSchema.Name,
                childTable.Name,
                foreignKey.Name);
            foreignKeyColumnsById.TryGetValue(
                foreignKey.ObjectId,
                out SqlServerForeignKeyColumnMetadata[]? pairs);
            pairs ??= [];
            keysByIndex.TryGetValue(
                (foreignKey.ReferencedObjectId, foreignKey.KeyIndexId),
                out BuiltKey? referencedKey);

            var sourceByReferencedColumn = new Dictionary<int, string>();
            bool pairsComplete = pairs.Length > 0;
            foreach (SqlServerForeignKeyColumnMetadata pair in pairs)
            {
                if (!columnsByCatalogId.TryGetValue(
                        (pair.ParentObjectId, pair.ParentColumnId),
                        out (SqlServerColumnMetadata Metadata, string Id) sourceColumn) ||
                    !sourceByReferencedColumn.TryAdd(
                        pair.ReferencedColumnId,
                        sourceColumn.Id))
                {
                    pairsComplete = false;
                }
            }

            var orderedSourceColumns = new List<string>();
            if (referencedKey is not null && referencedKey.MembershipComplete)
            {
                foreach (SqlServerIndexColumnMetadata referencedColumn in
                         referencedKey.OrderedColumns)
                {
                    if (!sourceByReferencedColumn.TryGetValue(
                            referencedColumn.ColumnId,
                            out string? sourceId))
                    {
                        pairsComplete = false;
                        break;
                    }
                    orderedSourceColumns.Add(sourceId);
                }
                pairsComplete &=
                    orderedSourceColumns.Count == pairs.Length &&
                    orderedSourceColumns.Count == referencedKey.OrderedColumns.Count &&
                    orderedSourceColumns.Distinct(StringComparer.Ordinal).Count() ==
                    orderedSourceColumns.Count;
            }
            else
            {
                pairsComplete = false;
            }

            if (referencedKey is null || !pairsComplete)
            {
                bool standaloneUniqueIndex =
                    indexesById.TryGetValue(
                        (foreignKey.ReferencedObjectId, foreignKey.KeyIndexId),
                        out SqlServerIndexMetadata? referencedIndex) &&
                    referencedIndex.IsUnique &&
                    !referencedIndex.IsPrimaryKey &&
                    !referencedIndex.IsUniqueConstraint;
                var unresolvedDependencies = pairs
                    .OrderBy(static item => item.ConstraintColumnId)
                    .Where(item => columnsByCatalogId.ContainsKey(
                        (item.ParentObjectId, item.ParentColumnId)))
                    .Select(item =>
                        columnsByCatalogId[(item.ParentObjectId, item.ParentColumnId)].Id)
                    .Distinct(StringComparer.Ordinal)
                    .ToList();
                if (indexObjectIds.TryGetValue(
                        (foreignKey.ReferencedObjectId, foreignKey.KeyIndexId),
                        out string? referencedIndexId))
                {
                    unresolvedDependencies.Add(referencedIndexId);
                }

                objects.Add(new MigrationCatalogObject
                {
                    ObjectId = foreignKeyId,
                    Kind = MigrationObjectKind.Other,
                    ParentObjectId = childTableId,
                    SourceNamespace = childSchema.Name,
                    SourceName = foreignKey.Name,
                    Facets =
                    [
                        Facet("kind", "sqlserver-unresolved-foreign-key"),
                        Facet("sqlServerObjectId", Invariant(foreignKey.ObjectId)),
                        Facet("sqlServerReferencedObjectId", Invariant(
                            foreignKey.ReferencedObjectId)),
                        Facet("sqlServerKeyIndexId", Invariant(foreignKey.KeyIndexId)),
                    ],
                    DependsOn = unresolvedDependencies
                        .Distinct(StringComparer.Ordinal)
                        .ToArray(),
                });
                diagnostics.Add(Diagnostic(
                    foreignKeyId,
                    standaloneUniqueIndex
                        ? "MIG-SQLSERVER-FK-UNIQUE-INDEX-TARGET-UNSUPPORTED-001"
                        : "MIG-SQLSERVER-FK-BINDING-UNKNOWN-001",
                    MigrationDiagnosticSeverity.Error,
                    standaloneUniqueIndex
                        ? MigrationCompatibilityStatus.Unsupported
                        : MigrationCompatibilityStatus.Unknown,
                    "The SQL Server foreign key could not be bound to a target key.",
                    standaloneUniqueIndex
                        ? "This checkpoint does not promote standalone unique indexes into logical target keys."
                        : "The referenced key or a complete ordered column pairing was not visible.",
                    "Create a reviewed primary or unique constraint and inspect again.",
                    canOverride: false));
                continue;
            }

            var foreignKeyFacets = new List<MigrationCatalogFacet>
            {
                Facet("timing", ForeignKeyTiming(foreignKey)),
                Facet("match", "simple"),
                Facet("deferrable", "false"),
                Facet("deferred", "false"),
                Facet("onDelete", ReferentialAction(foreignKey.DeleteAction)),
                Facet("sqlServerObjectId", Invariant(foreignKey.ObjectId)),
                Facet("sqlServerKeyIndexId", Invariant(foreignKey.KeyIndexId)),
                Facet("sqlServerDisabled", Boolean(foreignKey.IsDisabled)),
                Facet("sqlServerNotForReplication", Boolean(
                    foreignKey.IsNotForReplication)),
                Facet("sqlServerNotTrusted", Boolean(foreignKey.IsNotTrusted)),
                Facet("sqlServerDeleteAction", foreignKey.DeleteActionDescription),
                Facet("sqlServerUpdateAction", foreignKey.UpdateActionDescription),
                Facet("sqlServerSystemNamed", Boolean(foreignKey.IsSystemNamed)),
            };
            if (foreignKey.UpdateAction != 0)
            {
                foreignKeyFacets.Add(Facet(
                    "onUpdate",
                    ReferentialAction(foreignKey.UpdateAction)));
            }

            var members = orderedSourceColumns
                .Select((columnId, ordinal) => Member(
                    columnId,
                    MigrationObjectReferenceRoles.SourceColumn,
                    ordinal))
                .Append(Member(
                    referencedKey.ObjectId,
                    MigrationObjectReferenceRoles.ReferencedKey,
                    0))
                .ToArray();
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = foreignKeyId,
                Kind = MigrationObjectKind.ForeignKey,
                ParentObjectId = childTableId,
                SourceNamespace = childSchema.Name,
                SourceName = foreignKey.Name,
                Facets = foreignKeyFacets.AsReadOnly(),
                Members = members,
                DependsOn = orderedSourceColumns
                    .Append(referencedKey.ObjectId)
                    .Distinct(StringComparer.Ordinal)
                    .ToArray(),
            });

            if (ForeignKeyHasUnsupportedShape(foreignKey) ||
                !string.Equals(
                    referencedKey.EffectiveKind,
                    "primary",
                    StringComparison.Ordinal) &&
                !string.Equals(
                    referencedKey.EffectiveKind,
                    "unique",
                    StringComparison.Ordinal))
            {
                diagnostics.Add(Diagnostic(
                    foreignKeyId,
                    "MIG-SQLSERVER-FK-SHAPE-UNSUPPORTED-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unsupported,
                    "The SQL Server foreign key has unsupported enforcement semantics.",
                    "Disabled, untrusted, NOT FOR REPLICATION, SET NULL, SET DEFAULT, update-action, or unsupported referenced-key semantics cannot be silently lowered.",
                    "Rebuild a trusted immediate foreign key with a supported action and target key.",
                    canOverride: false));
            }
        }

        foreach (SqlServerCheckMetadata check in snapshot.Checks
                     .OrderBy(static item => item.ParentObjectId)
                     .ThenBy(static item => item.ObjectId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            (SqlServerTableMetadata table, string tableId) =
                tablesByObjectId[check.ParentObjectId];
            SqlServerSchemaMetadata schema = schemasById[table.SchemaId].Metadata;
            string checkId = ObjectId(
                "check",
                schema.Name,
                table.Name,
                check.Name);
            SqlServerScriptDomDefinitionAnalysis? checkAnalysis =
                GetScriptDomAnalysis(
                    scriptDomAnalysis,
                    SqlServerScriptDomDefinitionKind.CheckPredicate,
                    check.ObjectId,
                    subObjectId: 0,
                    check.Definition is not null);
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("sqlServerObjectId", Invariant(check.ObjectId)),
                Facet("sqlServerParentColumnId", Invariant(check.ParentColumnId)),
                Facet("sqlServerDisabled", Boolean(check.IsDisabled)),
                Facet("sqlServerNotForReplication", Boolean(
                    check.IsNotForReplication)),
                Facet("sqlServerNotTrusted", Boolean(check.IsNotTrusted)),
                Facet("sqlServerUsesDatabaseCollation", Boolean(
                    check.UsesDatabaseCollation)),
                Facet("sqlServerSystemNamed", Boolean(check.IsSystemNamed)),
            };
            AddDefinitionDigestFacets(
                facets,
                "sqlServerCheckDefinition",
                "csharpdb-sqlserver-check-definition/v1",
                check.DefinitionBytes,
                check.Definition);
            AddScriptDomFacets(
                facets,
                "sqlServerCheckTsqlAnalysis",
                "sqlServerCheckTsql",
                checkAnalysis);
            string[] dependencies =
                check.ParentColumnId > 0 &&
                columnsByCatalogId.TryGetValue(
                    (check.ParentObjectId, check.ParentColumnId),
                    out (SqlServerColumnMetadata Metadata, string Id) parentColumn)
                    ? [parentColumn.Id]
                    : [];
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = checkId,
                Kind = MigrationObjectKind.CheckConstraint,
                ParentObjectId = tableId,
                SourceNamespace = schema.Name,
                SourceName = check.Name,
                Facets = facets.AsReadOnly(),
                DependsOn = dependencies,
            });
            if (check.Definition is null)
            {
                diagnostics.Add(Diagnostic(
                    checkId,
                    "MIG-SQLSERVER-CHECK-DEFINITION-UNAVAILABLE-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unknown,
                    "The SQL Server check definition is unavailable.",
                    "The catalog reports a check constraint, but no predicate was visible for bounded syntax analysis.",
                    "Restore complete definition visibility and inspect again.",
                    canOverride: false));
            }
            else
            {
                AddScriptDomDiagnostic(
                    checkId,
                    "check predicate",
                    checkAnalysis!,
                    diagnostics);
            }
        }

        foreach (SqlServerSequenceMetadata sequence in snapshot.Sequences
                     .OrderBy(static item => item.SchemaId)
                     .ThenBy(static item => item.ObjectId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            (SqlServerSchemaMetadata schema, string namespaceId) =
                schemasById[sequence.SchemaId];
            string sequenceId = ObjectId("sequence", schema.Name, sequence.Name);
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = sequenceId,
                Kind = MigrationObjectKind.Sequence,
                ParentObjectId = namespaceId,
                SourceNamespace = schema.Name,
                SourceName = sequence.Name,
                Facets =
                [
                    Facet("sqlServerObjectId", Invariant(sequence.ObjectId)),
                    Facet("sqlServerTypeSchema", sequence.TypeSchema),
                    Facet("sqlServerTypeName", sequence.TypeName),
                    Facet("sqlServerSystemTypeName", sequence.SystemTypeName),
                    Facet("sqlServerPrecision", Invariant(sequence.Precision)),
                    Facet("sqlServerScale", Invariant(sequence.Scale)),
                    Facet("sqlServerStartValue", sequence.StartValue),
                    Facet("sqlServerIncrement", sequence.Increment),
                    Facet("sqlServerMinimumValue", sequence.MinimumValue),
                    Facet("sqlServerMaximumValue", sequence.MaximumValue),
                    Facet("sqlServerCycling", Boolean(sequence.IsCycling)),
                    Facet("sqlServerCached", Boolean(sequence.IsCached)),
                    Facet(
                        "sqlServerCacheSize",
                        sequence.CacheSize is null
                            ? null
                            : Invariant(sequence.CacheSize.Value)),
                ],
            });
            diagnostics.Add(Diagnostic(
                sequenceId,
                "MIG-SQLSERVER-SEQUENCE-UNSUPPORTED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "SQL Server sequences are not supported by the current target contract.",
                "Static sequence definition metadata is inventoried, but operational values are intentionally excluded and no target sequence lowering is advertised.",
                "Replace the sequence with a reviewed target key-generation strategy.",
                canOverride: false));
        }
    }

    private static void ValidateRelationalCounts(
        SqlServerCatalogSnapshot snapshot,
        SqlServerInspectionLimits limits)
    {
        if (snapshot.Keys.Count > limits.MaxKeys)
            throw LimitExceeded("key count");
        if (snapshot.Indexes.Count > limits.MaxIndexes)
            throw LimitExceeded("index count");
        if (snapshot.IndexColumns.Count > limits.MaxIndexColumns)
            throw LimitExceeded("index-column count");
        if (snapshot.ForeignKeys.Count > limits.MaxForeignKeys)
            throw LimitExceeded("foreign-key count");
        if (snapshot.ForeignKeyColumns.Count > limits.MaxForeignKeyColumns)
            throw LimitExceeded("foreign-key-column count");
        if (snapshot.Checks.Count > limits.MaxChecks)
            throw LimitExceeded("check count");
        if (snapshot.Sequences.Count > limits.MaxSequences)
            throw LimitExceeded("sequence count");

        long structuralRows = checked(
            (long)snapshot.Keys.Count +
            snapshot.Indexes.Count +
            snapshot.IndexColumns.Count +
            snapshot.ForeignKeys.Count +
            snapshot.ForeignKeyColumns.Count +
            snapshot.Checks.Count +
            snapshot.Sequences.Count);
        if (structuralRows > limits.MaxStructuralRowsTotal)
            throw LimitExceeded("aggregate structural-row count");

        ValidatePermissionCounts(snapshot.PermissionAuditBefore, limits);
        ValidatePermissionCounts(snapshot.PermissionAuditAfter, limits);
        long permissionRows = checked(
            (long)snapshot.PermissionAuditBefore.Tokens.Count +
            snapshot.PermissionAuditBefore.Denials.Count +
            snapshot.PermissionAuditAfter.Tokens.Count +
            snapshot.PermissionAuditAfter.Denials.Count);
        if (permissionRows > limits.MaxPermissionRowsTotal)
            throw LimitExceeded("aggregate permission-row count");
    }

    private static void ValidatePermissionCounts(
        SqlServerPermissionAuditMetadata audit,
        SqlServerInspectionLimits limits)
    {
        if (audit.Tokens.Count > limits.MaxUserTokens)
            throw LimitExceeded("permission-token count");
        if (audit.Denials.Count > limits.MaxPermissionDenials)
            throw LimitExceeded("permission-denial count");
    }

    private static void ValidateRelationalSnapshot(
        SqlServerCatalogSnapshot snapshot,
        IReadOnlySet<int> schemaIds,
        IReadOnlySet<int> tableIds,
        IReadOnlySet<(int ObjectId, int ColumnId)> columnIds,
        MetadataBudget budget,
        CancellationToken cancellationToken)
    {
        var indexes =
            new Dictionary<(int ObjectId, int IndexId), SqlServerIndexMetadata>();
        foreach (SqlServerIndexMetadata index in snapshot.Indexes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!tableIds.Contains(index.ObjectId) ||
                index.IndexId <= 0 ||
                index.Type == 0 ||
                !indexes.TryAdd((index.ObjectId, index.IndexId), index))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned index metadata.");
            }
            if ((index.IsPrimaryKey || index.IsUniqueConstraint) && !index.IsUnique)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned inconsistent constraint-index metadata.");
            }
            if (index.IsPrimaryKey && index.IsUniqueConstraint)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned ambiguous constraint-index metadata.");
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
                "index-filter");
            if (!index.HasFilter &&
                (index.FilterDefinitionBytes is not null ||
                 index.FilterDefinition is not null))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned inconsistent filtered-index metadata.");
            }
            if (index.CompressionDelay < 0)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned invalid index compression-delay metadata.");
            }
        }

        var indexColumnIds = new HashSet<(int ObjectId, int IndexId, int IndexColumnId)>();
        foreach (SqlServerIndexColumnMetadata column in snapshot.IndexColumns)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!indexes.TryGetValue(
                    (column.ObjectId, column.IndexId),
                    out SqlServerIndexMetadata? index) ||
                column.IndexColumnId <= 0 ||
                !indexColumnIds.Add((
                    column.ObjectId,
                    column.IndexId,
                    column.IndexColumnId)))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned index-column metadata.");
            }
            if (column.ColumnId < 0 ||
                column.ColumnId == 0 && !IsHeapRid(index, column) ||
                column.ColumnId > 0 &&
                !columnIds.Contains((column.ObjectId, column.ColumnId)))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned an index column outside its table.");
            }
        }

        var keys = new HashSet<int>();
        var keyIndexes = new HashSet<(int ObjectId, int IndexId)>();
        foreach (SqlServerKeyMetadata key in snapshot.Keys)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (key.ObjectId <= 0 ||
                !keys.Add(key.ObjectId) ||
                !tableIds.Contains(key.ParentObjectId) ||
                key.UniqueIndexId <= 0 ||
                !keyIndexes.Add((key.ParentObjectId, key.UniqueIndexId)) ||
                key.Type is not ("PK" or "UQ"))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned key metadata.");
            }
            budget.Add(key.Name, isName: true);
        }

        var foreignKeys = new Dictionary<int, SqlServerForeignKeyMetadata>();
        foreach (SqlServerForeignKeyMetadata foreignKey in snapshot.ForeignKeys)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (foreignKey.ObjectId <= 0 ||
                !foreignKeys.TryAdd(foreignKey.ObjectId, foreignKey) ||
                !tableIds.Contains(foreignKey.ParentObjectId) ||
                !tableIds.Contains(foreignKey.ReferencedObjectId) ||
                foreignKey.KeyIndexId <= 0 ||
                foreignKey.DeleteAction > 3 ||
                foreignKey.UpdateAction > 3)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned foreign-key metadata.");
            }
            budget.Add(foreignKey.Name, isName: true);
            budget.Add(foreignKey.DeleteActionDescription);
            budget.Add(foreignKey.UpdateActionDescription);
        }

        var foreignKeyColumns = new HashSet<(int ConstraintObjectId, int Ordinal)>();
        foreach (SqlServerForeignKeyColumnMetadata column in snapshot.ForeignKeyColumns)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!foreignKeys.TryGetValue(
                    column.ConstraintObjectId,
                    out SqlServerForeignKeyMetadata? foreignKey) ||
                column.ConstraintColumnId <= 0 ||
                !foreignKeyColumns.Add((
                    column.ConstraintObjectId,
                    column.ConstraintColumnId)) ||
                column.ParentObjectId != foreignKey.ParentObjectId ||
                column.ReferencedObjectId != foreignKey.ReferencedObjectId ||
                !columnIds.Contains((column.ParentObjectId, column.ParentColumnId)) ||
                !columnIds.Contains((
                    column.ReferencedObjectId,
                    column.ReferencedColumnId)))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned foreign-key-column metadata.");
            }
        }

        var checks = new HashSet<int>();
        foreach (SqlServerCheckMetadata check in snapshot.Checks)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (check.ObjectId <= 0 ||
                !checks.Add(check.ObjectId) ||
                !tableIds.Contains(check.ParentObjectId) ||
                check.ParentColumnId < 0 ||
                check.ParentColumnId > 0 &&
                !columnIds.Contains((check.ParentObjectId, check.ParentColumnId)))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned check metadata.");
            }
            budget.Add(check.Name, isName: true);
            budget.ReserveExpression(check.DefinitionBytes);
            budget.AddExpression(check.Definition);
            ValidateDefinitionLength(
                check.Definition,
                check.DefinitionBytes,
                "check");
        }

        var sequences = new HashSet<int>();
        var sequenceNames = new HashSet<(int SchemaId, string Name)>();
        foreach (SqlServerSequenceMetadata sequence in snapshot.Sequences)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (sequence.ObjectId <= 0 ||
                !sequences.Add(sequence.ObjectId) ||
                !schemaIds.Contains(sequence.SchemaId) ||
                !sequenceNames.Add((sequence.SchemaId, sequence.Name)) ||
                sequence.Precision is < 1 or > 38 ||
                sequence.Scale != 0 ||
                !IsCanonicalInteger(sequence.StartValue) ||
                !IsCanonicalInteger(sequence.Increment) ||
                sequence.Increment == "0" ||
                sequence.MinimumValue is not null &&
                !IsCanonicalInteger(sequence.MinimumValue) ||
                sequence.MaximumValue is not null &&
                !IsCanonicalInteger(sequence.MaximumValue) ||
                sequence.CacheSize < 0)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned sequence metadata.");
            }
            budget.Add(sequence.Name, isName: true);
            budget.Add(sequence.TypeSchema, isName: true);
            budget.Add(sequence.TypeName, isName: true);
            budget.Add(sequence.SystemTypeName, isName: true);
            budget.Add(sequence.StartValue);
            budget.Add(sequence.Increment);
            budget.Add(sequence.MinimumValue);
            budget.Add(sequence.MaximumValue);
        }

        ValidatePermissionAudit(snapshot.PermissionAuditBefore, budget);
        ValidatePermissionAudit(snapshot.PermissionAuditAfter, budget);
    }

    private static void ValidatePermissionAudit(
        SqlServerPermissionAuditMetadata audit,
        MetadataBudget budget)
    {
        if (!audit.Attempted &&
            (audit.Tokens.Count != 0 || audit.Denials.Count != 0))
        {
            throw new SqlServerMigrationException(
                "SQL Server returned permission evidence for an audit that was not attempted.");
        }

        var tokens = new HashSet<(int PrincipalId, string Type, string Usage)>();
        foreach (SqlServerUserTokenMetadata token in audit.Tokens)
        {
            if (token.PrincipalId < 0 ||
                !tokens.Add((token.PrincipalId, token.Type, token.Usage)))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate or invalid permission-token metadata.");
            }
            budget.Add(token.Type);
            budget.Add(token.Usage);
        }

        var denials = new HashSet<(
            byte Class,
            int MajorId,
            int MinorId,
            string PermissionName,
            int GranteePrincipalId,
            string TokenUsage)>();
        foreach (SqlServerPermissionDenyMetadata denial in audit.Denials)
        {
            if (denial.MajorId < 0 ||
                denial.MinorId < 0 ||
                denial.GranteePrincipalId < 0 ||
                !denials.Add((
                    denial.Class,
                    denial.MajorId,
                    denial.MinorId,
                    denial.PermissionName,
                    denial.GranteePrincipalId,
                    denial.TokenUsage)))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate or invalid permission-denial metadata.");
            }
            budget.Add(denial.PermissionName);
            budget.Add(denial.TokenUsage);
        }
    }

    private static void AddPermissionQualificationDiagnostics(
        SqlServerCatalogSnapshot snapshot,
        string databaseId,
        ICollection<MigrationDiagnostic> diagnostics)
    {
        if (snapshot.Database.IsSysAdmin == true)
            return;

        if (!snapshot.PermissionAuditBefore.Attempted ||
            !snapshot.PermissionAuditAfter.Attempted)
        {
            diagnostics.Add(Diagnostic(
                databaseId,
                "MIG-SQLSERVER-PERMISSION-AUDIT-MISSING-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "Effective SQL Server permission evidence was not captured.",
                "A bounded before-and-after token and DENY scan is required to qualify least-privilege metadata visibility.",
                "Re-run with the production reader and required catalog permissions.",
                canOverride: false));
        }
        else if (!PermissionAuditsEqual(
                     snapshot.PermissionAuditBefore,
                     snapshot.PermissionAuditAfter))
        {
            diagnostics.Add(Diagnostic(
                databaseId,
                "MIG-SQLSERVER-PERMISSION-AUDIT-DRIFT-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "SQL Server permission evidence changed during inspection.",
                "The effective database token or explicit DENY set differed between the preflight and final read.",
                "Stabilize role membership and grants, then inspect again.",
                canOverride: false));
        }

        if (HasRelevantMetadataDeny(snapshot.PermissionAuditBefore) ||
            HasRelevantMetadataDeny(snapshot.PermissionAuditAfter))
        {
            diagnostics.Add(Diagnostic(
                databaseId,
                "MIG-SQLSERVER-PERMISSION-DENY-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "An effective SQL Server DENY prevents proving complete metadata visibility.",
                "At least one database-, schema-, or object-level DENY applies to an effective database token; column-only denials are not treated as metadata blockers.",
                "Use a dedicated read-only principal without conflicting metadata denials.",
                canOverride: false));
        }

        if (snapshot.Instance.ProductMajorVersion >= 16 &&
            snapshot.Database.IsSysAdmin != true &&
            snapshot.Database.HasViewSecurityDefinition != true)
        {
            diagnostics.Add(Diagnostic(
                databaseId,
                "MIG-SQLSERVER-VIEW-SECURITY-DEFINITION-MISSING-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "SQL Server security-definition visibility is not proven.",
                "SQL Server 2022 and later require separate security-definition visibility before the permission catalog can be treated as complete.",
                "Grant and verify the least privilege needed to inspect security definitions, then re-run analysis.",
                canOverride: false));
        }

        if (snapshot.Database.HasViewDefinition == false ||
            snapshot.Schemas.Any(static item => item.HasViewDefinition == false) ||
            snapshot.Tables.Any(static item => item.HasViewDefinition == false))
        {
            diagnostics.Add(Diagnostic(
                databaseId,
                "MIG-SQLSERVER-VIEW-DEFINITION-MISSING-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "SQL Server VIEW DEFINITION evidence is incomplete.",
                "The database or at least one visible schema or table explicitly reports that its definition cannot be viewed.",
                "Grant VIEW DEFINITION at a reviewed scope and re-run the inventory.",
                canOverride: false));
        }
    }

    private static bool HasRelevantMetadataDeny(
        SqlServerPermissionAuditMetadata audit) =>
        audit.Denials.Any(static denial =>
            denial.Class == 0 ||
            denial.Class == 3 ||
            denial.Class == 1 && denial.MinorId == 0);

    private static bool PermissionAuditsEqual(
        SqlServerPermissionAuditMetadata left,
        SqlServerPermissionAuditMetadata right) =>
        string.Equals(
            PermissionAuditDigest(left),
            PermissionAuditDigest(right),
            StringComparison.Ordinal);

    private static string PermissionAuditDigest(
        SqlServerPermissionAuditMetadata audit) =>
        "sha256:" + SqlServerStableDigest.Sequence(
            "csharpdb-sqlserver-permission-audit/v1",
            PermissionAuditFields(audit));

    private static IEnumerable<string?> PermissionAuditFields(
        SqlServerPermissionAuditMetadata audit)
    {
        yield return Boolean(audit.Attempted);
        foreach (SqlServerUserTokenMetadata token in audit.Tokens
                     .OrderBy(static item => item.PrincipalId)
                     .ThenBy(static item => item.Type, StringComparer.Ordinal)
                     .ThenBy(static item => item.Usage, StringComparer.Ordinal))
        {
            yield return "token";
            yield return Invariant(token.PrincipalId);
            yield return token.Type;
            yield return token.Usage;
        }
        foreach (SqlServerPermissionDenyMetadata denial in audit.Denials
                     .OrderBy(static item => item.Class)
                     .ThenBy(static item => item.MajorId)
                     .ThenBy(static item => item.MinorId)
                     .ThenBy(static item => item.PermissionName, StringComparer.Ordinal)
                     .ThenBy(static item => item.GranteePrincipalId)
                     .ThenBy(static item => item.TokenUsage, StringComparer.Ordinal))
        {
            yield return "deny";
            yield return Invariant(denial.Class);
            yield return Invariant(denial.MajorId);
            yield return Invariant(denial.MinorId);
            yield return denial.PermissionName;
            yield return Invariant(denial.GranteePrincipalId);
            yield return denial.TokenUsage;
        }
    }

    private static IEnumerable<string?> RelationalSnapshotFields(
        SqlServerCatalogSnapshot snapshot)
    {
        foreach (SqlServerKeyMetadata key in snapshot.Keys
                     .OrderBy(static item => item.ParentObjectId)
                     .ThenBy(static item => item.ObjectId))
        {
            yield return "key";
            yield return Invariant(key.ObjectId);
            yield return Invariant(key.ParentObjectId);
            yield return key.Name;
            yield return key.Type;
            yield return Invariant(key.UniqueIndexId);
            yield return Boolean(key.IsSystemNamed);
        }
        foreach (SqlServerIndexMetadata index in snapshot.Indexes
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.IndexId))
        {
            yield return "index";
            yield return Invariant(index.ObjectId);
            yield return Invariant(index.IndexId);
            yield return index.Name;
            yield return Invariant(index.Type);
            yield return index.TypeDescription;
            yield return Boolean(index.IsUnique);
            yield return Invariant(index.DataSpaceId);
            yield return index.DataSpaceName;
            yield return index.DataSpaceType;
            yield return Boolean(index.IgnoreDuplicateKey);
            yield return Boolean(index.IsPrimaryKey);
            yield return Boolean(index.IsUniqueConstraint);
            yield return Invariant(index.FillFactor);
            yield return Boolean(index.IsPadded);
            yield return Boolean(index.IsDisabled);
            yield return Boolean(index.IsHypothetical);
            yield return Boolean(index.AllowRowLocks);
            yield return Boolean(index.AllowPageLocks);
            yield return Boolean(index.HasFilter);
            yield return index.FilterDefinitionBytes is null
                ? null
                : Invariant(index.FilterDefinitionBytes.Value);
            yield return index.FilterDefinition;
            yield return index.CompressionDelay is null
                ? null
                : Invariant(index.CompressionDelay.Value);
            yield return Boolean(index.SuppressDuplicateKeyMessages);
            yield return Boolean(index.OptimizeForSequentialKey);
        }
        foreach (SqlServerIndexColumnMetadata column in snapshot.IndexColumns
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.IndexId)
                     .ThenBy(static item => item.IndexColumnId))
        {
            yield return "index-column";
            yield return Invariant(column.ObjectId);
            yield return Invariant(column.IndexId);
            yield return Invariant(column.IndexColumnId);
            yield return Invariant(column.ColumnId);
            yield return Invariant(column.KeyOrdinal);
            yield return Invariant(column.PartitionOrdinal);
            yield return Boolean(column.IsDescending);
            yield return Boolean(column.IsIncluded);
        }
        foreach (SqlServerForeignKeyMetadata foreignKey in snapshot.ForeignKeys
                     .OrderBy(static item => item.ParentObjectId)
                     .ThenBy(static item => item.ObjectId))
        {
            yield return "foreign-key";
            yield return Invariant(foreignKey.ObjectId);
            yield return Invariant(foreignKey.ParentObjectId);
            yield return Invariant(foreignKey.ReferencedObjectId);
            yield return Invariant(foreignKey.KeyIndexId);
            yield return foreignKey.Name;
            yield return Boolean(foreignKey.IsDisabled);
            yield return Boolean(foreignKey.IsNotForReplication);
            yield return Boolean(foreignKey.IsNotTrusted);
            yield return Invariant(foreignKey.DeleteAction);
            yield return foreignKey.DeleteActionDescription;
            yield return Invariant(foreignKey.UpdateAction);
            yield return foreignKey.UpdateActionDescription;
            yield return Boolean(foreignKey.IsSystemNamed);
        }
        foreach (SqlServerForeignKeyColumnMetadata column in snapshot.ForeignKeyColumns
                     .OrderBy(static item => item.ConstraintObjectId)
                     .ThenBy(static item => item.ConstraintColumnId))
        {
            yield return "foreign-key-column";
            yield return Invariant(column.ConstraintObjectId);
            yield return Invariant(column.ConstraintColumnId);
            yield return Invariant(column.ParentObjectId);
            yield return Invariant(column.ParentColumnId);
            yield return Invariant(column.ReferencedObjectId);
            yield return Invariant(column.ReferencedColumnId);
        }
        foreach (SqlServerCheckMetadata check in snapshot.Checks
                     .OrderBy(static item => item.ParentObjectId)
                     .ThenBy(static item => item.ObjectId))
        {
            yield return "check";
            yield return Invariant(check.ObjectId);
            yield return Invariant(check.ParentObjectId);
            yield return check.Name;
            yield return Invariant(check.ParentColumnId);
            yield return Boolean(check.IsDisabled);
            yield return Boolean(check.IsNotForReplication);
            yield return Boolean(check.IsNotTrusted);
            yield return check.DefinitionBytes is null
                ? null
                : Invariant(check.DefinitionBytes.Value);
            yield return check.Definition;
            yield return Boolean(check.UsesDatabaseCollation);
            yield return Boolean(check.IsSystemNamed);
        }
        foreach (SqlServerSequenceMetadata sequence in snapshot.Sequences
                     .OrderBy(static item => item.SchemaId)
                     .ThenBy(static item => item.ObjectId))
        {
            yield return "sequence";
            yield return Invariant(sequence.ObjectId);
            yield return Invariant(sequence.SchemaId);
            yield return sequence.Name;
            yield return sequence.TypeSchema;
            yield return sequence.TypeName;
            yield return sequence.SystemTypeName;
            yield return Invariant(sequence.Precision);
            yield return Invariant(sequence.Scale);
            yield return sequence.StartValue;
            yield return sequence.Increment;
            yield return sequence.MinimumValue;
            yield return sequence.MaximumValue;
            yield return Boolean(sequence.IsCycling);
            yield return Boolean(sequence.IsCached);
            yield return sequence.CacheSize is null
                ? null
                : Invariant(sequence.CacheSize.Value);
        }

        yield return "permission-audit-before";
        foreach (string? field in PermissionAuditFields(snapshot.PermissionAuditBefore))
            yield return field;
        yield return "permission-audit-after";
        foreach (string? field in PermissionAuditFields(snapshot.PermissionAuditAfter))
            yield return field;
    }

    private static void AddPhysicalIndexFacets(
        ICollection<MigrationCatalogFacet> facets,
        SqlServerIndexMetadata index,
        IReadOnlyList<SqlServerIndexColumnMetadata> columns)
    {
        facets.Add(Facet("sqlServerIndexId", Invariant(index.IndexId)));
        facets.Add(Facet("sqlServerIndexType", index.TypeDescription));
        facets.Add(Facet("sqlServerIndexTypeCode", Invariant(index.Type)));
        facets.Add(Facet("sqlServerUnique", Boolean(index.IsUnique)));
        facets.Add(Facet("sqlServerDataSpaceId", Invariant(index.DataSpaceId)));
        facets.Add(Facet("sqlServerDataSpaceName", index.DataSpaceName));
        facets.Add(Facet("sqlServerDataSpaceType", index.DataSpaceType));
        facets.Add(Facet("sqlServerIgnoreDuplicateKey", Boolean(
            index.IgnoreDuplicateKey)));
        facets.Add(Facet("sqlServerPrimaryKeyIndex", Boolean(index.IsPrimaryKey)));
        facets.Add(Facet("sqlServerUniqueConstraintIndex", Boolean(
            index.IsUniqueConstraint)));
        facets.Add(Facet("sqlServerFillFactor", Invariant(index.FillFactor)));
        facets.Add(Facet("sqlServerPadded", Boolean(index.IsPadded)));
        facets.Add(Facet("sqlServerDisabled", Boolean(index.IsDisabled)));
        facets.Add(Facet("sqlServerHypothetical", Boolean(index.IsHypothetical)));
        facets.Add(Facet("sqlServerAllowRowLocks", Boolean(index.AllowRowLocks)));
        facets.Add(Facet("sqlServerAllowPageLocks", Boolean(index.AllowPageLocks)));
        facets.Add(Facet("sqlServerHasFilter", Boolean(index.HasFilter)));
        facets.Add(Facet(
            "sqlServerCompressionDelay",
            index.CompressionDelay is null
                ? null
                : Invariant(index.CompressionDelay.Value)));
        facets.Add(Facet("sqlServerSuppressDuplicateKeyMessages", Boolean(
            index.SuppressDuplicateKeyMessages)));
        facets.Add(Facet("sqlServerOptimizeForSequentialKey", Boolean(
            index.OptimizeForSequentialKey)));
        facets.Add(Facet("sqlServerPartitioned", Boolean(
            columns.Any(static item => item.PartitionOrdinal > 0))));
        if (columns.Any(item => IsHeapRid(index, item)))
            facets.Add(Facet("sqlServerHeapRid", "true"));
    }

    private static void AddFilterAnalysisDiagnostic(
        string objectId,
        SqlServerIndexMetadata? index,
        SqlServerScriptDomDefinitionAnalysis? analysis,
        ICollection<MigrationDiagnostic> diagnostics)
    {
        if (index?.HasFilter != true)
            return;

        if (index.FilterDefinition is null)
        {
            diagnostics.Add(Diagnostic(
                objectId,
                "MIG-SQLSERVER-FILTER-DEFINITION-UNAVAILABLE-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "The SQL Server filtered-index predicate is unavailable.",
                "The catalog reports a filtered index, but no predicate was visible for bounded syntax analysis.",
                "Restore complete definition visibility and inspect again.",
                canOverride: false));
            return;
        }

        AddScriptDomDiagnostic(
            objectId,
            "filtered-index predicate",
            analysis!,
            diagnostics);
    }

    private static void AddDefinitionDigestFacets(
        ICollection<MigrationCatalogFacet> facets,
        string facetPrefix,
        string digestDomain,
        long? sourceBytes,
        string? definition)
    {
        facets.Add(Facet(
            facetPrefix + "SourceBytes",
            sourceBytes is null ? "unknown" : Invariant(sourceBytes.Value)));
        if (definition is null)
            return;
        facets.Add(Facet(
            facetPrefix + "Digest",
            "sha256:" + SqlServerStableDigest.Text(digestDomain, definition)));
        facets.Add(Facet(
            facetPrefix + "Length",
            Invariant(definition.Length)));
    }

    private static bool HasContiguousKeyOrdinals(
        IReadOnlyList<SqlServerIndexColumnMetadata> columns) =>
        columns.Count > 0 &&
        columns
            .Select(static item => (int)item.KeyOrdinal)
            .SequenceEqual(Enumerable.Range(1, columns.Count));

    private static bool HasUnsupportedConstraintBackingShape(
        SqlServerIndexMetadata index,
        IReadOnlyList<SqlServerIndexColumnMetadata> columns) =>
        index.Type is not (1 or 2) ||
        index.IsDisabled ||
        index.IsHypothetical ||
        index.IgnoreDuplicateKey ||
        index.HasFilter ||
        columns.Any(item =>
            item.IsIncluded ||
            item.PartitionOrdinal > 0 ||
            item.ColumnId <= 0 && !IsHeapRid(index, item) ||
            IsUnclassifiedRowstoreColumn(index, item));

    private static bool IsHeapRid(
        SqlServerIndexMetadata index,
        SqlServerIndexColumnMetadata column) =>
        index.Type == 2 &&
        column.ColumnId == 0 &&
        column.KeyOrdinal == 0 &&
        column.PartitionOrdinal == 0 &&
        !column.IsDescending &&
        !column.IsIncluded;

    private static bool IsUnclassifiedRowstoreColumn(
        SqlServerIndexMetadata index,
        SqlServerIndexColumnMetadata column) =>
        index.Type is 1 or 2 &&
        column.KeyOrdinal == 0 &&
        !column.IsIncluded &&
        column.PartitionOrdinal == 0 &&
        !IsHeapRid(index, column);

    private static bool ForeignKeyHasUnsupportedShape(
        SqlServerForeignKeyMetadata foreignKey) =>
        foreignKey.IsDisabled ||
        foreignKey.IsNotForReplication ||
        foreignKey.IsNotTrusted ||
        foreignKey.DeleteAction is 2 or 3 ||
        foreignKey.UpdateAction != 0;

    private static string ForeignKeyTiming(
        SqlServerForeignKeyMetadata foreignKey) =>
        foreignKey.IsDisabled
            ? "sqlserver-disabled"
            : foreignKey.IsNotTrusted
                ? "sqlserver-untrusted"
                : foreignKey.IsNotForReplication
                    ? "sqlserver-not-for-replication"
                    : "immediate";

    private static string ReferentialAction(byte action) =>
        action switch
        {
            0 => "restrict",
            1 => "cascade",
            2 => "set-null",
            3 => "set-default",
            _ => "unknown",
        };

    private static string IndexKind(SqlServerIndexMetadata index) =>
        index.Type switch
        {
            2 => "standard",
            1 => "clustered",
            3 => "xml",
            4 => "spatial",
            5 => "clustered-columnstore",
            6 => "nonclustered-columnstore",
            7 => "hash",
            9 => "json",
            _ => "sqlserver-index-" + Invariant(index.Type),
        };

    private static bool IsCanonicalInteger(string value) =>
        BigInteger.TryParse(
            value,
            NumberStyles.AllowLeadingSign,
            CultureInfo.InvariantCulture,
            out BigInteger parsed) &&
        string.Equals(
            parsed.ToString(CultureInfo.InvariantCulture),
            value,
            StringComparison.Ordinal);

    private static MigrationObjectReference Member(
        string objectId,
        string role,
        int ordinal) =>
        new()
        {
            ObjectId = objectId,
            Role = role,
            Ordinal = ordinal,
        };

    private sealed record BuiltKey(
        string ObjectId,
        string EffectiveKind,
        bool MembershipComplete,
        IReadOnlyList<SqlServerIndexColumnMetadata> OrderedColumns);
}
